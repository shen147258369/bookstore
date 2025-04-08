import uuid
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError, DuplicateKeyError
from be.model import error
from be.model.db_conn import DBConn

class Buyer(DBConn):
    def __init__(self):
        super().__init__()

    def new_order(
        self, user_id: str, store_id: str, id_and_count: List[Tuple[str, int]]
    ) -> Tuple[int, str, str]:
        order_id = ""
        try:
            # 前置检查
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + ("",)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + ("",)

            order_id = f"{user_id}_{store_id}_{uuid.uuid1().hex}"
            items = []
            
            # 处理每个商品项（使用原子操作替代事务）
            for book_id, count in id_and_count:
                # 原子性库存检查和扣减
                try:
                    result = self.stores.find_one_and_update(
                        {
                            "store_id": store_id,
                            "books.book_id": book_id,
                            "books.stock": {"$gte": count}
                        },
                        {"$inc": {"books.$.stock": -count}},  # 扣减库存
                        return_document=ReturnDocument.AFTER  # 返回更新后的文档
                    )
                    
                    if not result:
                        logging.error(f"Failed to update stock for book_id={book_id}. Not enough stock.")
                        return error.error_stock_level_low(book_id) + (order_id,)

                    book = result["books"][0]  # 获取更新后的书籍信息
                    items.append({
                        "book_id": book_id,
                        "count": count,
                        "price": book["price"]
                    })
                except PyMongoError as e:
                    logging.error(f"Error updating stock for book_id={book_id}: {str(e)}")
                    return 528, str(e), order_id

            try:
                self.orders.insert_one({
                    "order_id": order_id,
                    "user_id": user_id,
                    "store_id": store_id,
                    "items": items,
                    "total": sum(item["price"] * item["count"] for item in items),
                    "status": "unpaid",
                    "create_time": datetime.utcnow(),
                    "update_time": datetime.utcnow()
                })
            except PyMongoError as e:
                logging.error(f"Error inserting order {order_id}: {str(e)}")
                return 528, str(e), order_id

            return 200, "ok", order_id

        except DuplicateKeyError as e:
            logging.error(f"Order ID conflict: {order_id}, {str(e)}")
            return 529, "Order ID conflict", ""
        except PyMongoError as e:
            logging.error(f"MongoDB error in new_order: {str(e)}")
            return 528, str(e), ""
        except Exception as e:
            logging.error(f"Unexpected error in new_order: {str(e)}")
            return 530, str(e), ""


    def payment(self, user_id: str, password: str, order_id: str) -> Tuple[int, str]:
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    # 获取并验证订单
                    order = self.orders.find_one(
                        {"order_id": order_id},
                        projection=["user_id", "status", "total", "store_id"],
                        session=session
                    )
                    if not order:
                        return error.error_invalid_order_id(order_id)
                    
                    if order["user_id"] != user_id:
                        return error.error_authorization_fail()
                    
                    if order["status"] != "unpaid":
                        return error.error_order_status(order_id)

                    # 验证支付密码
                    user = self.users.find_one(
                        {"user_id": user_id},
                        projection=["password", "balance"],
                        session=session
                    )
                    if not user or user["password"] != password:
                        return error.error_authorization_fail()

                    # 获取商家信息
                    store = self.user_store.find_one(
                        {"store_id": order["store_id"]},
                        projection=["user_id"],
                        session=session
                    )
                    if not store:
                        return error.error_non_exist_store_id(order["store_id"])
                    seller_id = store["user_id"]

                    # 余额检查与更新
                    if user["balance"] < order["total"]:
                        return error.error_not_sufficient_funds(order_id)

                    # 使用细粒度锁保护余额更新
                    with self.lock:
                        # 扣买家余额
                        buyer_update = self.users.update_one(
                            {"user_id": user_id, "balance": {"$gte": order["total"]}},
                            {"$inc": {"balance": -order["total"]}},
                            session=session
                        )
                        if buyer_update.modified_count == 0:
                            session.abort_transaction()
                            return error.error_not_sufficient_funds(order_id)

                        # 加商家余额
                        seller_update = self.users.update_one(
                            {"user_id": seller_id},
                            {"$inc": {"balance": order["total"]}},
                            session=session
                        )
                        if seller_update.modified_count == 0:
                            session.abort_transaction()
                            return error.error_non_exist_user_id(seller_id)

                        # 更新订单状态
                        self.orders.update_one(
                            {"order_id": order_id},
                            {
                                "$set": {
                                    "status": "paid",
                                    "timestamps.updated": datetime.utcnow()
                                }
                            },
                            session=session
                        )

                    session.commit_transaction()
                    return 200, "ok"

        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}", exc_info=True)
            return 530, str(e)

    def add_funds(self, user_id: str, password: str, add_value: int) -> Tuple[int, str]:
        """用户充值"""
        try:
            # 验证用户凭证
            user = self.users.find_one(
                {"user_id": user_id},
                projection=["password"]
            )
            if not user or user["password"] != password:
                return error.error_authorization_fail()

            # 原子性更新余额
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

    def receive_order(self, user_id: str, order_id: str) -> Tuple[int, str]:
        """确认收货"""
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    # 验证订单状态
                    order = self.orders.find_one_and_update(
                        {
                            "order_id": order_id,
                            "user_id": user_id,
                            "status": "shipped"
                        },
                        {
                            "$set": {
                                "status": "completed",
                                "timestamps.updated": datetime.utcnow()
                            }
                        },
                        projection=["_id"],
                        session=session,
                        return_document=True
                    )
                    
                    if not order:
                        return error.error_invalid_order_id(order_id)
                    
                    session.commit_transaction()
                    return 200, "ok"

        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)

    def get_order_status(self, user_id: str, order_id: str) -> Tuple[int, str, Optional[str]]:
        """查询订单状态"""
        try:
            order = self.orders.find_one(
                {"order_id": order_id},
                projection=["user_id", "status"]
            )
            if not order:
                return *error.error_invalid_order_id(order_id), None  # type: ignore
            
            if order["user_id"] != user_id:
                return *error.error_authorization_fail(), None  # type: ignore
            
            return 200, "ok", order["status"]

        except PyMongoError as e:
            logging.error(f"Database error: {str(e)}")
            return 500, f"Database error: {str(e)}", None
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 500, f"Unexpected error: {str(e)}", None

    def cancel_order(self, user_id: str, order_id: str) -> Tuple[int, str]:
        """取消订单"""
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    # 获取并验证订单
                    order = self.orders.find_one(
                        {"order_id": order_id},
                        projection=["user_id", "status", "store_id", "items"],
                        session=session
                    )
                    if not order:
                        return error.error_invalid_order_id(order_id)
                    
                    if order["user_id"] != user_id:
                        return error.error_authorization_fail()
                    
                    if order["status"] != "unpaid":
                        return error.error_order_status(order_id)

                    # 恢复库存
                    for item in order["items"]:
                        self.stores.update_one(
                            {"store_id": order["store_id"], "books.book_id": item["book_id"]},
                            {"$inc": {"books.$.stock": item["count"]}},
                            session=session
                        )

                    # 更新订单状态
                    self.orders.update_one(
                        {"order_id": order_id},
                        {
                            "$set": {
                                "status": "cancelled",
                                "timestamps.updated": datetime.utcnow()
                            }
                        },
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

class OrderCleaner(threading.Thread):
    """自动取消未支付订单的后台任务"""
    def __init__(self, interval_seconds: int = 300):
        super().__init__(daemon=True)
        self.interval = interval_seconds
        self.db = DBConn()

    def run(self):
        while True:
            try:
                timeout = datetime.utcnow() - timedelta(minutes=15)
                
                with self.db.client.start_session() as session:
                    with session.start_transaction():
                        # 查询超时订单
                        orders = list(self.db.orders.find({
                            "status": "unpaid",
                            "timestamps.created": {"$lte": timeout}
                        }, projection=["store_id", "items"], session=session))

                        if orders:
                            # 批量恢复库存
                            for order in orders:
                                bulk_ops = []
                                for item in order["items"]:
                                    bulk_ops.append((
                                        {"store_id": order["store_id"], "books.book_id": item["book_id"]},
                                        {"$inc": {"books.$.stock": item["count"]}}
                                    ))
                                
                                # 批量执行库存更新
                                for query, update in bulk_ops:
                                    self.db.stores.update_one(query, update, session=session)

                            # 批量更新订单状态
                            order_ids = [o["order_id"] for o in orders]
                            self.db.orders.update_many(
                                {"order_id": {"$in": order_ids}},
                                {"$set": {"status": "auto_cancelled"}},
                                session=session
                            )

                        session.commit_transaction()
                        if orders:
                            logging.info(f"Auto-cancelled {len(orders)} orders")

            except PyMongoError as e:
                logging.error(f"Cleaner error: {str(e)}")
            finally:
                time.sleep(self.interval)