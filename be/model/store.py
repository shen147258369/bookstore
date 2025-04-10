import logging
import os
from pymongo import MongoClient
import threading

class Store:
    def __init__(self, db_path):
        self.client = MongoClient()
        self.db = self.client['bookstore_lx']
        self.init_tables()

    def init_tables(self):
        try:
            self.db.create_collection('user')
            self.db['user'].create_index('user_id', unique=True)

            self.db.create_collection('user_store')
            self.db['user_store'].create_index([('user_id', 1), ('store_id', 1)], unique=True)

            self.db['store'].create_index([
                ('book_info.title', 'text'),
                ('book_info.tags', 'text'),
                ('book_info.content', 'text'),
                ('book_info.book_intro', 'text'),
                ('book_info.author', 'text'),  
                ('book_info.publisher', 'text')  
            ],
            name='book_text_index',
            weights={
                'book_info.title': 10,
                'book_info.tags': 5,
                'book_info.content': 1,
                'book_info.book_intro': 3,
                'book_info.author': 8,  
                'book_info.publisher': 6  
            })
            self.db.create_collection('new_order')
            self.db['new_order'].create_index('order_id', unique=True)
            self.db['new_order'].create_index('user_id')
            self.db['new_order'].create_index('store_id')
            self.db['new_order'].create_index('status')
            self.db['new_order'].create_index('order_time')

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