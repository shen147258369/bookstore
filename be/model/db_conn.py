from pymongo import MongoClient, ReturnDocument
from pymongo.errors import PyMongoError, DuplicateKeyError
from be.model import error
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DBConn:
    def __init__(self):
        try:
            self.client = MongoClient('mongodb://localhost:27017/', connectTimeoutMS=30000, socketTimeoutMS=None)
            self.db = self.client['bookstore']
            
            # 集合定义
            self.users = self.db.users          # 用户集合
            self.stores = self.db.stores        # 店铺库存集合
            self.orders = self.db.orders        # 订单集合
            self.books = self.db.books          # 书籍元数据集合
            self.user_store = self.db.user_store # 用户-店铺关系
            self._create_indexes()
            logger.info("Successfully connected to MongoDB")
            
        except PyMongoError as e:
            logger.error(f"MongoDB connection failed: {str(e)}")
            raise error.InternalError("Database connection failed")

    def _create_indexes(self):
        """创建必要索引（幂等操作）"""
        # 用户唯一索引
        self.users.create_index("user_id", unique=True, name="user_id_unique")
        
        # 店铺-书籍联合唯一索引
        self.stores.create_index(
            [("store_id", 1), ("books.book_id", 1)],
            unique=True,
            name="store_book_unique"
        )
        
        # 订单相关索引
        self.orders.create_index("order_id", unique=True, name="order_id_unique")
        self.orders.create_index("user_id", name="user_id_index")
        self.orders.create_index([("status", 1), ("timestamp", 1)], name="status_time_compound")
        
        # 全文搜索索引（书籍信息）
        self.books.create_index([
            ("title", "text"),
            ("author", "text"),
            ("content", "text"),
            ("tags", "text")
        ], name="book_search_index", default_language="english")

    def user_id_exist(self, user_id: str) -> bool:
        try:
            return bool(self.users.find_one(
                {"user_id": user_id},
                projection={"_id": 0}
            ))
        except PyMongoError as e:
            logger.error(f"User existence check failed: {str(e)}")
            raise error.InternalError("Database error")

    def book_id_exist(self, store_id: str, book_id: str) -> bool:
        try:
            # 检查店铺库存中的书籍存在性
            return bool(self.stores.find_one({
                "store_id": store_id,
                "books.book_id": book_id
            }, projection={"_id": 0}))
        except PyMongoError as e:
            logger.error(f"Book existence check failed: {str(e)}")
            raise error.InternalError("Database error")

    def store_id_exist(self, store_id: str) -> bool:
        try:
            # 检查店铺是否已注册
            return bool(self.user_store.find_one(
                {"store_id": store_id},
                projection={"_id": 0}
            ))
        except PyMongoError as e:
            logger.error(f"Store existence check failed: {str(e)}")
            raise error.InternalError("Database error")

    def get_book_stock(self, store_id: str, book_id: str) -> int:
        """获取指定书籍库存"""
        try:
            store = self.stores.find_one(
                {"store_id": store_id, "books.book_id": book_id},
                {"_id": 0, "books.$": 1}
            )
            return store["books"][0]["stock"] if store else 0
        except PyMongoError as e:
            logger.error(f"Get stock failed: {str(e)}")
            raise error.InternalError("Database error")

    def update_stock(self, store_id: str, book_id: str, delta: int) -> bool:
        """原子性更新库存"""
        try:
            result = self.stores.update_one(
                {"store_id": store_id, "books.book_id": book_id},
                {"$inc": {"books.$.stock": delta}}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Stock update failed: {str(e)}")
            raise error.InternalError("Database error")

    def create_order(self, order_data: dict) -> str:
        """创建订单（使用事务）"""
        session = self.client.start_session()
        try:
            with session.start_transaction():
                # 检查并扣减库存
                for item in order_data["items"]:
                    book_id = item["book_id"]
                    store_id = order_data["store_id"]
                    quantity = item["quantity"]

                    # 原子性库存检查
                    updated = self.stores.find_one_and_update(
                        {
                            "store_id": store_id,
                            "books.book_id": book_id,
                            "books.stock": {"$gte": quantity}
                        },
                        {"$inc": {"books.$.stock": -quantity}},
                        session=session,
                        return_document=ReturnDocument.AFTER
                    )
                    
                    if not updated:
                        raise error.StockError(f"Insufficient stock for {book_id}")

                # 插入订单记录
                result = self.orders.insert_one(order_data, session=session)
                return str(result.inserted_id)
                
        except PyMongoError as e:
            session.abort_transaction()
            logger.error(f"Order creation failed: {str(e)}")
            raise error.InternalError("Database error")
        finally:
            session.end_session()

    def close(self):
        try:
            self.client.close()
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")