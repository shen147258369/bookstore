from pymongo import MongoClient
from pymongo.errors import PyMongoError
from be.model import error

class DBConn:
    def __init__(self):
        try:
            # 连接到 MongoDB
            self.client = MongoClient('mongodb://localhost:27017/')
            self.db = self.client['bookstore_lx']  # 使用你的数据库名
            
            # 定义集合
            self.users = self.db.users
            self.stores = self.db.stores
            self.user_store = self.db.user_store
            
            # 创建必要索引（只需执行一次）
            self._create_indexes()
            
        except PyMongoError as e:
            print(f"MongoDB 连接失败: {str(e)}")
            raise error.InternalError("Database connection failed")

    def _create_indexes(self):
        """创建必要索引（幂等操作）"""
        # 用户唯一索引
        self.users.create_index("user_id", unique=True, background=True)
        # 店铺图书复合索引
        self.stores.create_index([("store_id", 1), ("books.book_id", 1)], background=True)
        # 店铺唯一索引
        self.user_store.create_index("store_id", unique=True, background=True)

    def user_id_exist(self, user_id: str) -> bool:
        try:
            return bool(self.users.find_one(
                {"user_id": user_id},
                projection={"_id": 0, "user_id": 1}
            ))
        except PyMongoError as e:
            print(f"查询用户存在性失败: {str(e)}")
            raise error.InternalError("Database error")

    def book_id_exist(self, store_id: str, book_id: str) -> bool:
        try:
            return bool(self.stores.find_one(
                {
                    "store_id": store_id,
                    "books": {"$elemMatch": {"book_id": book_id}}
                },
                projection={"_id": 0, "books.$": 1}
            ))
        except PyMongoError as e:
            print(f"查询书籍存在性失败: {str(e)}")
            raise error.InternalError("Database error")

    def store_id_exist(self, store_id: str) -> bool:
        try:
            return bool(self.user_store.find_one(
                {"store_id": store_id},
                projection={"_id": 0, "store_id": 1}
            ))
        except PyMongoError as e:
            print(f"查询店铺存在性失败: {str(e)}")
            raise error.InternalError("Database error")

    # 关闭连接方法（按需调用）
    def close(self):
        self.client.close()