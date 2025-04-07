import json
import logging
from typing import Tuple, Union
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from be.model import error
from be.model.db_conn import DBConn

class Seller(DBConn):
    def __init__(self):
        super().__init__()  # 初始化 MongoDB 连接

    def create_store(self, user_id: str, store_id: str) -> Tuple[int, str]:
        """创建新店铺"""
        try:
            # 验证用户存在性
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            
            # 检查店铺是否已存在
            if self.store_id_exist(store_id):
                return error.error_exist_store_id(store_id)
            
            # 插入店铺元数据
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
            # 验证用户和店铺存在性
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)
            
            # 检查图书是否已存在
            if self.book_id_exist(store_id, book_id):
                return error.error_exist_book_id(book_id)
            
            # 解析图书信息
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
                    "books.book_id": book_id
                },
                {"$inc": {"books.$.stock": add_stock_level}}
            )
            
            if result.modified_count == 0:
                return error.error_non_exist_book_id(book_id)
            
            return 200, "ok"
            
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)