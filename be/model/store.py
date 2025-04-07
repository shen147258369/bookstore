import logging
import os
import threading
from be.model.db_conn import DBConn  # 引入已定义的 MongoDB 连接类

class Store:
    def __init__(self, db_path: str):
        # MongoDB 无需指定路径，连接通过 URI 配置（假设 db_path 在此处兼容原有接口，实际使用可忽略）
        self.db_conn = DBConn()  # 初始化 MongoDB 连接（包含索引创建）

    def init_tables(self):
        """MongoDB 无需显式创建表，集合自动创建，此处保留接口兼容"""
        pass  # 索引创建已在 DBConn._create_indexes 中完成

# 全局变量和初始化逻辑调整（兼容原有接口）
database_instance: Store = None
init_completed_event = threading.Event()

def init_database(db_path: str):
    global database_instance
    database_instance = Store(db_path)
    init_completed_event.set()  # 标记初始化完成

def get_db_conn():
    """返回 DBConn 实例（包含 MongoDB 连接）"""
    global database_instance
    return database_instance.db_conn  # 直接返回 DBConn 对象，而非原始连接