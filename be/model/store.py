import logging
import os
from pymongo import MongoClient
import threading

class Store:
    def __init__(self, db_path):
        self.client = MongoClient()
        self.db = self.client['bookstore']  # 使用 'bookstore' 作为数据库名
        self.init_tables()

    def init_tables(self):
        try:
            # 创建用户集合
            self.db.create_collection('user')
            self.db['user'].create_index('user_id', unique=True)

            # 创建用户店铺集合
            self.db.create_collection('user_store')
            self.db['user_store'].create_index([('user_id', 1), ('store_id', 1)], unique=True)

            # 创建店铺集合
            self.db.create_collection('store')
            self.db['store'].create_index([('store_id', 1), ('book_id', 1)], unique=True)

            # 创建新订单集合
            self.db.create_collection('new_order')
            self.db['new_order'].create_index('order_id', unique=True)
            self.db['new_order'].create_index('user_id')
            self.db['new_order'].create_index('store_id')
            self.db['new_order'].create_index('status')
            self.db['new_order'].create_index('order_time')

            # 创建新订单详情集合
            self.db.create_collection('new_order_detail')
            self.db['new_order_detail'].create_index([('order_id', 1), ('book_id', 1)], unique=True)

        except Exception as e:
            logging.error(e)

    def get_db_conn(self):
        return self.db

database_instance: Store = None
# global variable for database sync
init_completed_event = threading.Event()

def init_database(db_path):
    global database_instance
    database_instance = Store(db_path)

def get_db_conn():
    global database_instance
    return database_instance.get_db_conn()