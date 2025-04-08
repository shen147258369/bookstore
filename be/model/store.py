import threading
from pymongo import MongoClient, ASCENDING
from be.model.db_conn import DBConn


class Store:
    def __init__(self, db_path: str):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['bookstore']
        self.init_tables()

    def init_tables(self):
        try:
            self.db.users.create_index(
                [("user_id", ASCENDING)],
                unique=True,
                name="user_id_unique"
            )
            self.db.user_store.create_index(
                [("store_id", ASCENDING)],
                unique=True,
                name="store_id_unique"
            )
            self.db.stores.create_index(
                [("store_id", ASCENDING), ("books.book_id", ASCENDING)],
                unique=True,
                name="store_book_unique"
            )
            self.db.orders.create_index(
                [("order_id", ASCENDING)],
                unique=True,
                name="order_id_unique"
            )
            self.db.orders.create_index(
                [("user_id", ASCENDING)],
                name="user_id_index"
            )
            self.db.orders.create_index(
                [("status", ASCENDING), ("timestamp", ASCENDING)],
                name="status_time_compound"
            )
            self.db.books.create_index([
                ("title", "text"),
                ("author", "text"),
                ("content", "text"),
                ("tags", "text")
            ], name="book_search_index", default_language="english")

        except Exception as e:
            print(e)

    def get_db_conn(self) -> DBConn:
        return DBConn()

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