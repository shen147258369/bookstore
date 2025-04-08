from datetime import datetime
import json
import logging
from typing import Tuple, Union
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from be.model import error
from be.model.db_conn import DBConn

class Seller(DBConn):
    def __init__(self):
        super().__init__()

    def create_store(self, user_id: str, store_id: str) -> Tuple[int, str]:
        try:

            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)

            if self.store_id_exist(store_id):
                return error.error_exist_store_id(store_id)

            self.user_store.insert_one({
                "store_id": store_id,
                "user_id": user_id,
                "books": []
            })

            return 200, "ok"

        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)

    def add_book(
        self,
        user_id: str,
        store_id: str,
        book_id: str,
        book_json_str: str,
        stock_level: int
    ) -> Tuple[int, str]:
        """添加图书到店铺"""
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)

            if self.book_id_exist(store_id, book_id):
                return error.error_exist_book_id(book_id)

            try:
                book_info = json.loads(book_json_str)
            except json.JSONDecodeError:
                return 531, "Invalid book JSON format"

            # 添加图书到店铺库存
            self.stores.update_one(
                {"store_id": store_id},
                {"$push": {
                    "books": {
                        "book_id": book_id,
                        "info": book_info,
                        "stock": stock_level,
                        "price": book_info.get("price", 0)
                    }
                }}
            )

            return 200, "ok"

        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)

    def add_stock_level(
        self,
        user_id: str,
        store_id: str,
        book_id: str,
        add_stock_level: int
    ) -> Tuple[int, str]:
        """增加库存"""
        try:
            # 验证用户和店铺存在性
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)

            # 检查图书是否存在
            if not self.book_id_exist(store_id, book_id):
                return error.error_non_exist_book_id(book_id)

            # 原子操作增加库存
            result = self.stores.update_one(
                {
                    "store_id": store_id,
                    "books.book_id": book_id  # 确保匹配嵌套文档
                },
                {
                    "$inc": {"books.$.stock": add_stock_level},
                    "$set": {"last_updated": datetime.utcnow()}
                }
            )
            
            if result.matched_count == 0:
                return error.error_non_exist_book_id(book_id)
                
            return 200, "ok"

        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)

    def ship_order(self, seller_id: str, store_id: str, order_id: str) -> Tuple[int, str]:
        """发货订单"""
        try:
            # 获取订单信息
            order = self.new_order.find_one({
                "order_id": order_id,
                "store_id": store_id
            })
            if not order:
                return error.error_invalid_order_id(order_id)

            status = order.get("status")

            # 验证卖家权限
            store_owner = self.user_store.find_one({
                "user_id": seller_id,
                "store_id": store_id
            })
            if not store_owner:
                return error.error_authorization_fail()

            # 检查状态
            if status != 'paid':
                return error.error_order_status(order_id)

            # 更新状态
            self.new_order.update_one(
                {"order_id": order_id},
                {"$set": {"status": "shipped"}}
            )

            return 200, "ok"

        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)
