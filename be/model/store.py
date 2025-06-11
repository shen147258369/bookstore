import logging
import mysql.connector
import threading
from contextlib import closing

class Store:
    def __init__(self, host="localhost", user="stu", password="123456", database="bookstore_lx"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.init_tables()

    def get_connection(self):
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def get_connection_no_db(self):
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password
        )

    def column_exists(self, cursor, table_name, column_name):
        """Check if a column exists in a table"""
        cursor.execute(f"""
            SELECT COUNT(1) 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s 
            AND column_name = %s
        """, (self.database, table_name, column_name))
        return cursor.fetchone()[0] > 0

    def index_exists(self, cursor, table_name, index_name):
        """Check if an index exists on a table"""
        cursor.execute(f"""
            SELECT COUNT(1) 
            FROM information_schema.statistics 
            WHERE table_schema = %s 
            AND table_name = %s 
            AND index_name = %s
        """, (self.database, table_name, index_name))
        return cursor.fetchone()[0] > 0

    def fulltext_index_exists(self, cursor, table_name, index_name):
        """Check if a fulltext index exists on a table"""
        cursor.execute(f"""
            SELECT COUNT(1) 
            FROM information_schema.statistics 
            WHERE table_schema = %s 
            AND table_name = %s 
            AND index_name = %s
            AND index_type = 'FULLTEXT'
        """, (self.database, table_name, index_name))
        return cursor.fetchone()[0] > 0

    def init_tables(self):
        with closing(self.get_connection_no_db()) as conn:
            with closing(conn.cursor()) as cursor:
                try:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                    cursor.execute(f"USE {self.database}")

                    # Create books table first since other tables reference it
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS books (
                            id VARCHAR(255) PRIMARY KEY,
                            title VARCHAR(255),
                            author VARCHAR(255),
                            publisher VARCHAR(255),
                            original_title VARCHAR(255),
                            translator VARCHAR(255),
                            pub_year VARCHAR(128),
                            pages INT,
                            price FLOAT,
                            currency_unit VARCHAR(128),
                            binding VARCHAR(50),
                            isbn VARCHAR(50),
                            author_intro LONGTEXT,
                            book_intro LONGTEXT,
                            content LONGTEXT,
                            pictures LONGBLOB
                        )
                    """)

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id VARCHAR(255) PRIMARY KEY,
                            password_hash VARCHAR(255) NOT NULL,
                            token TEXT,
                            terminal VARCHAR(255),
                            balance DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS stores (
                            store_id VARCHAR(255) PRIMARY KEY,
                            user_id VARCHAR(255),
                            store_name VARCHAR(255) NOT NULL,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                            UNIQUE KEY unique_user_store (user_id, store_id)
                        )
                    """)

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS store_inventory (
                            inventory_id INT PRIMARY KEY AUTO_INCREMENT,
                            store_id VARCHAR(255),
                            book_id VARCHAR(255),
                            stock_quantity INT NOT NULL DEFAULT 0,
                            book_price DECIMAL(10,2) NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
                            FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
                            UNIQUE KEY unique_store_book (store_id, book_id)
                        )
                    """)

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS orders (
                            order_id VARCHAR(255) PRIMARY KEY,
                            user_id VARCHAR(255),
                            store_id VARCHAR(255),
                            order_status VARCHAR(50) NOT NULL DEFAULT 'pending',
                            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            total_amount DECIMAL(10,2),
                            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                            FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
                            INDEX idx_user_id (user_id),
                            INDEX idx_store_id (store_id),
                            INDEX idx_status (order_status),
                            INDEX idx_order_time (create_time)
                        )
                    """)

                    # Check and add ship_time and receive_time columns if they don't exist
                    if not self.column_exists(cursor, "orders", "ship_time"):
                        try:
                            cursor.execute("ALTER TABLE orders ADD COLUMN ship_time TIMESTAMP NULL DEFAULT NULL")
                            logging.info("Added ship_time column to orders table")
                        except Exception as e:
                            logging.warning(f"添加 ship_time 列失败: {e}")
                    else:
                        logging.info("Column ship_time already exists")

                    if not self.column_exists(cursor, "orders", "receive_time"):
                        try:
                            cursor.execute("ALTER TABLE orders ADD COLUMN receive_time TIMESTAMP NULL DEFAULT NULL")
                            logging.info("Added receive_time column to orders table")
                        except Exception as e:
                            logging.warning(f"添加 receive_time 列失败: {e}")
                    else:
                        logging.info("Column receive_time already exists")

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS order_details (
                            detail_id INT PRIMARY KEY AUTO_INCREMENT,
                            order_id VARCHAR(255),
                            book_id VARCHAR(255),
                            quantity INT NOT NULL,
                            unit_price DECIMAL(10,2) NOT NULL,
                            FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
                            FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
                            UNIQUE KEY unique_order_book (order_id, book_id)
                        )
                    """)

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS book_search_index (
                            book_id VARCHAR(255) PRIMARY KEY,
                            search_content TEXT,
                            FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
                            FULLTEXT(search_content)
                        )
                    """)

                    # Check and create indexes only if they don't exist
                    if not self.index_exists(cursor, "books", "idx_title"):
                        try:
                            cursor.execute("CREATE INDEX idx_title ON books(title)")
                            logging.info("Created index idx_title on books(title)")
                        except Exception as e:
                            logging.warning(f"创建索引 idx_title 失败: {e}")
                    else:
                        logging.info("Index idx_title already exists")

                    if not self.index_exists(cursor, "books", "idx_author"):
                        try:
                            cursor.execute("CREATE INDEX idx_author ON books(author)")
                            logging.info("Created index idx_author on books(author)")
                        except Exception as e:
                            logging.warning(f"创建索引 idx_author 失败: {e}")
                    else:
                        logging.info("Index idx_author already exists")

                    if not self.index_exists(cursor, "books", "idx_publisher"):
                        try:
                            cursor.execute("CREATE INDEX idx_publisher ON books(publisher)")
                            logging.info("Created index idx_publisher on books(publisher)")
                        except Exception as e:
                            logging.warning(f"创建索引 idx_publisher 失败: {e}")
                    else:
                        logging.info("Index idx_publisher already exists")

                    if not self.fulltext_index_exists(cursor, "books", "idx_fulltext"):
                        try:
                            cursor.execute("""
                                ALTER TABLE books 
                                ADD FULLTEXT idx_fulltext (title, author, publisher, book_intro, content)
                            """)
                            logging.info("Created fulltext index idx_fulltext on books")
                        except Exception as e:
                            logging.warning(f"添加全文索引 idx_fulltext 失败: {e}")
                    else:
                        logging.info("Fulltext index idx_fulltext already exists")
                    
                    if not self.fulltext_index_exists(cursor, "books", "idx_content_fulltext"):
                        try:
                            cursor.execute("""
                                ALTER TABLE books 
                                ADD FULLTEXT idx_content_fulltext (content)
                            """)
                            logging.info("Created fulltext index idx_content_fulltext on books(content)")
                        except Exception as e:
                            logging.warning(f"添加全文索引 idx_content_fulltext 失败: {e}")
                    else:
                        logging.info("Fulltext index idx_content_fulltext already exists")
                    

                    conn.commit()

                except Exception as e:
                    logging.error(f"创建数据库或表失败: {e}")
                    conn.rollback()

    def get_db_conn(self):
        return self.get_connection()


database_instance: Store = None
init_completed_event = threading.Event()

def init_database(host="localhost", user="stu", password="123456", database="bookstore_lx"):
    global database_instance
    database_instance = Store(host=host, user=user, password=password, database=database)
    init_completed_event.set()

def get_db_conn():
    global database_instance
    if database_instance is None:
        raise RuntimeError("数据库未初始化，请先调用 init_database()")
    return database_instance.get_db_conn()