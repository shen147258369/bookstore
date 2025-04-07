import logging
import os
import threading
from pymongo import MongoClient, ASCENDING
from be.model.db_conn import DBConn

class Store:
    def __init__(self, db_path: str):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['bookstore']
        self.init_tables()

    def init_tables(self):
        """初始化MongoDB集合和索引（幂等操作）"""
        try:
            # 用户集合
            self._create_index_with_cleanup(
                collection=self.db.users,
                index_name="user_id_unique",
                keys=[("user_id", ASCENDING)],
                unique=True
            )

            # 用户-店铺关系集合
            self._create_index_with_cleanup(
                collection=self.db.user_store,
                index_name="store_id_unique",
                keys=[("store_id", ASCENDING)],
                unique=True
            )

            # 店铺库存集合
            self._create_index_with_cleanup(
                collection=self.db.stores,
                index_name="store_book_unique",
                keys=[("store_id", ASCENDING), ("books.book_id", ASCENDING)],
                unique=True
            )

            # 订单集合
            self._create_index_with_cleanup(
                collection=self.db.orders,
                index_name="order_id_unique",
                keys=[("order_id", ASCENDING)],
                unique=True
            )
            self._create_index_with_cleanup(
                collection=self.db.orders,
                index_name="user_id_index",
                keys=[("user_id", ASCENDING)]
            )
            self._create_index_with_cleanup(
                collection=self.db.orders,
                index_name="status_create_time_compound",  # 修改索引名称
                keys=[("status", ASCENDING), ("create_time", ASCENDING)]
            )

            # 书籍元数据集合
            self._create_index_with_cleanup(
                collection=self.db.books,
                index_name="book_search_index",
                keys=[
                    ("title", "text"),
                    ("author", "text"),
                    ("content", "text"),
                    ("tags", "text")
                ],
                **{"default_language": "english"}
            )

            logging.info("Database indexes initialized successfully")
        except Exception as e:
            logging.error(f"Initialize database failed: {str(e)}")
            raise

    def _create_index_with_cleanup(self, collection, index_name, keys, **kwargs):
        """安全的索引创建方法（处理名称冲突）"""
        try:
            # 删除可能存在的旧索引
            existing_indexes = collection.index_information()
            if index_name in existing_indexes:
                # 检查现有索引是否匹配
                existing_keys = existing_indexes[index_name]['key']
                if existing_keys != keys:
                    collection.drop_index(index_name)
                    logging.info(f"Dropped conflicting index: {index_name}")
                else:
                    return  # 索引已存在且相同，无需操作

            # 创建新索引
            collection.create_index(keys, name=index_name, **kwargs)
            logging.info(f"Created index: {index_name}")

        except Exception as e:
            logging.error(f"Failed to create index {index_name}: {str(e)}")
            raise

    def get_db_conn(self) -> DBConn:
        return DBConn()


# 全局实例和初始化同步
database_instance: Store = None
init_completed_event = threading.Event()

def init_database(db_path: str):
    global database_instance
    database_instance = Store(db_path)
    init_completed_event.set()

def get_db_conn() -> DBConn:
    global database_instance
    if not init_completed_event.is_set():
        init_completed_event.wait()
    return database_instance.get_db_conn()