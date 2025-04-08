from pymongo import MongoClient, ReturnDocument
from pymongo.errors import PyMongoError, DuplicateKeyError
from be.model import error

class DBConn:
    def __init__(self):
        try:
            self.client = MongoClient('mongodb://localhost:27017/', maxPoolSize=100, minPoolSize=10,
                                      connectTimeoutMS=30000, socketTimeoutMS=None)
            self.db = self.client['bookstore']
            self.users = self.db.users
            self.stores = self.db.stores
            self.orders = self.db.orders
            self.books = self.db.books
            self.user_store = self.db.user_store
            self._create_indexes()
        except PyMongoError as e:
            raise error.InternalError("Database connection failed")

    def _create_indexes(self):
        self.users.create_index("user_id", unique=True, name="user_id_unique")
        self.stores.create_index(
            [("store_id", 1), ("books.book_id", 1)],
            unique=True,
            name="store_book_unique"
        )
        self.orders.create_index("order_id", unique=True, name="order_id_unique")
        self.orders.create_index("user_id", name="user_id_index")
        self.orders.create_index([("status", 1), ("timestamp", 1)], name="status_time_compound")

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
            raise error.InternalError("Database error")

    def book_id_exist(self, store_id: str, book_id: str) -> bool:
        try:
            return bool(self.stores.find_one({
                "store_id": store_id,
                "books.book_id": book_id
            }, projection={"_id": 0}))
        except PyMongoError as e:
            raise error.InternalError("Database error")

    def store_id_exist(self, store_id: str) -> bool:
        try:
            # 检查店铺是否已注册
            return bool(self.user_store.find_one(
                {"store_id": store_id},
                projection={"_id": 0}
            ))
        except PyMongoError as e:
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
            raise error.InternalError("Database error")

    def update_stock(self, store_id: str, book_id: str, delta: int) -> bool:
        try:
            result = self.stores.update_one(
                {"store_id": store_id, "books.book_id": book_id},
                {"$inc": {"books.$.stock": delta}}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            raise error.InternalError("Database error")

    def create_order(self, order_data: dict) -> str:
        session = self.client.start_session()
        try:
            with session.start_transaction():
                # 检查并扣减库存
                for item in order_data["items"]:
                    book_id = item["book_id"]
                    store_id = order_data["store_id"]
                    quantity = item["quantity"]

                    # 获取当前库存
                    current_stock = self.get_book_stock(store_id, book_id)

                    if current_stock < quantity:
                        raise error.error_stock_level_low(book_id)  # 返回517错误

                    # 原子性库存更新
                    updated = self.stores.find_one_and_update(
                        {
                            "store_id": store_id,
                            "books.book_id": book_id,
                            "books.stock": {"$gte": quantity}  # 确保库存足够
                        },
                        {"$inc": {"books.$.stock": -quantity}},
                        session=session,
                        return_document=ReturnDocument.AFTER
                    )

                    # 如果库存不足或更新失败，抛出库存不足错误
                    if not updated:
                        raise error.error_stock_level_low(book_id)  # 返回517错误

                # 插入订单记录
                result = self.orders.insert_one(order_data, session=session)
                return str(result.inserted_id)

        except PyMongoError as e:
            session.abort_transaction()
            raise error.InternalError("Database error")
        finally:
            session.end_session()

    def close(self):
        try:
            self.client.close()
        except Exception as e:
            pass
    