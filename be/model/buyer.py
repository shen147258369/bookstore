import uuid
import logging
from typing import List, Tuple
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from be.model import error
from be.model.db_conn import DBConn

class Buyer(DBConn):
    def __init__(self):
        super().__init__()  # 初始化 MongoDB 连接

    def new_order(
        self, user_id: str, store_id: str, id_and_count: List[Tuple[str, int]]
    ) -> Tuple[int, str, str]:
        order_id = ""
        try:
            # 验证用户和店铺存在性
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)
            
            # 生成唯一订单ID
            order_id = f"{user_id}_{store_id}_{uuid.uuid1().hex}"
            
            # 开启事务会话
            with self.client.start_session() as session:
                with session.start_transaction():
                    # 处理每个商品
                    total_items = []
                    for book_id, count in id_and_count:
                        # 查询库存
                        store = self.stores.find_one(
                            {
                                "store_id": store_id,
                                "books.book_id": book_id
                            },
                            projection={"books.$": 1},
                            session=session
                        )
                        
                        if not store or len(store["books"]) == 0:
                            return error.error_non_exist_book_id(book_id) + (order_id,)
                        
                        book = store["books"][0]
                        if book["stock"] < count:
                            return error.error_stock_level_low(book_id) + (order_id,)
                        
                        # 扣减库存
                        result = self.stores.update_one(
                            {
                                "store_id": store_id,
                                "books.book_id": book_id,
                                "books.stock": {"$gte": count}
                            },
                            {"$inc": {"books.$.stock": -count}},
                            session=session
                        )
                        
                        if result.modified_count == 0:
                            return error.error_stock_level_low(book_id) + (order_id,)
                        
                        # 记录订单详情
                        total_items.append({
                            "book_id": book_id,
                            "count": count,
                            "price": book["price"]
                        })
                    
                    # 创建订单文档
                    order_doc = {
                        "order_id": order_id,
                        "user_id": user_id,
                        "store_id": store_id,
                        "items": total_items,
                        "status": "unpaid",
                        "total": sum(item["price"]*item["count"] for item in total_items)
                    }
                    
                    # 插入订单
                    self.orders.insert_one(order_doc, session=session)
                    
                    # 提交事务
                    session.commit_transaction()
            
            return 200, "ok", order_id
            
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e), ""
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e), ""

    def payment(self, user_id: str, password: str, order_id: str) -> Tuple[int, str]:
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    # 获取订单信息
                    order = self.orders.find_one(
                        {"order_id": order_id},
                        session=session
                    )
                    
                    if not order:
                        return error.error_invalid_order_id(order_id)
                    
                    if order["user_id"] != user_id:
                        return error.error_authorization_fail()
                    
                    # 验证用户密码
                    user = self.users.find_one(
                        {"user_id": user_id},
                        projection={"password": 1, "balance": 1},
                        session=session
                    )
                    
                    if not user:
                        return error.error_non_exist_user_id(user_id)
                    
                    if user["password"] != password:
                        return error.error_authorization_fail()
                    
                    # 验证店铺所有者
                    store = self.user_store.find_one(
                        {"store_id": order["store_id"]},
                        projection={"user_id": 1},
                        session=session
                    )
                    
                    if not store:
                        return error.error_non_exist_store_id(order["store_id"])
                    
                    seller_id = store["user_id"]
                    
                    # 检查余额
                    total_price = order["total"]
                    if user["balance"] < total_price:
                        return error.error_not_sufficient_funds(order_id)
                    
                    # 扣款转账
                    # 扣买家余额
                    buyer_update = self.users.update_one(
                        {"user_id": user_id, "balance": {"$gte": total_price}},
                        {"$inc": {"balance": -total_price}},
                        session=session
                    )
                    
                    if buyer_update.modified_count == 0:
                        return error.error_not_sufficient_funds(order_id)
                    
                    # 加卖家余额
                    seller_update = self.users.update_one(
                        {"user_id": seller_id},
                        {"$inc": {"balance": total_price}},
                        session=session
                    )
                    
                    if seller_update.modified_count == 0:
                        return error.error_non_exist_user_id(seller_id)
                    
                    # 更新订单状态
                    self.orders.update_one(
                        {"order_id": order_id},
                        {"$set": {"status": "paid"}},
                        session=session
                    )
                    
                    session.commit_transaction()
            
            return 200, "ok"
            
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)

    def add_funds(self, user_id: str, password: str, add_value: int) -> Tuple[int, str]:
        try:
            # 验证密码
            user = self.users.find_one(
                {"user_id": user_id},
                projection={"password": 1}
            )
            if not user:
                return error.error_authorization_fail()
            if user["password"] != password:
                return error.error_authorization_fail()
            
            # 原子操作增加余额
            result = self.users.update_one(
                {"user_id": user_id},
                {"$inc": {"balance": add_value}}
            )
            
            if result.modified_count == 0:
                return error.error_non_exist_user_id(user_id)
            
            return 200, "ok"
            
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)