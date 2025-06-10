import pymongo
import mysql.connector
from tqdm import tqdm
import re
from bson.binary import Binary

def migrate():
    # MongoDB 连接
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["bookstore_lx"]
    mongo_col = mongo_db["books"]

    # MySQL 连接（修改你的密码）
    mysql_conn = mysql.connector.connect(
    host="localhost", 
    user="stu",
    password="123456",
    )
    cursor = mysql_conn.cursor()

    # 创建数据库和表
    cursor.execute("CREATE DATABASE IF NOT EXISTS bookstore_lx")
    cursor.execute("USE bookstore_lx")

    # 删除所有旧表（有依赖关系，注意顺序）
    drop_order = [
        "order_details", "orders",
        "store_inventory", "stores",
        "book_search_index",
        "book_tags", "tags", "books",
        "users"
    ]

    for table in drop_order:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception as e:
            print(f"⚠️ 删除表 {table} 时出错：{str(e)}")

    # 创建表结构，将 id 改为 VARCHAR(255)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id VARCHAR(255) PRIMARY KEY,  # 改为字符串类型
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
        CREATE TABLE IF NOT EXISTS tags (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(225) UNIQUE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS book_tags (
            book_id VARCHAR(255),  # 改为字符串类型
            tag_id INT,
            PRIMARY KEY (book_id, tag_id),
            FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
        )
    """)


    mysql_conn.commit()

    # 读取所有书籍记录（包括 content 和 pictures）
    docs = list(mongo_col.find({}))
    print(f"📦 共读取 {len(docs)} 条图书记录，开始迁移...")

    for doc in tqdm(docs, desc="迁移中", unit="本书"):
        try:
            # 确保id是字符串类型
            book_id = str(doc["id"])
            
            # 获取 content 和 pictures 字段
            content = doc.get("content")
            pictures = doc.get("pictures")
            if isinstance(pictures, Binary):
                pictures = bytes(pictures)  # 确保是原始字节流
            elif isinstance(pictures, list):  # 多张图情况
                pictures = b''.join([bytes(p) for p in pictures if isinstance(p, Binary)])

            # 插入 books 表
            cursor.execute("""
                INSERT INTO books (
                    id, title, author, publisher, original_title, translator,
                    pub_year, pages, price, currency_unit, binding, isbn,
                    author_intro, book_intro, content, pictures
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                book_id, doc["title"], doc["author"], doc["publisher"], doc["original_title"],
                doc.get("translator"), doc["pub_year"], doc["pages"], doc["price"],
                doc["currency_unit"], doc["binding"], doc["isbn"],
                doc.get("author_intro"), doc.get("book_intro"), content, pictures
            ))

            # 处理 tags
            for raw_tag_str in doc.get("tags", []):
                tag_candidates = re.split(r'[\n，]', raw_tag_str)
                for tag in tag_candidates:
                    tag = tag.strip()
                    tag = re.sub(r'[^\w\s\u4e00-\u9fff\-]', '', tag)
                    if not tag:
                        continue

                    cursor.execute("SELECT id FROM tags WHERE name = %s", (tag,))
                    result = cursor.fetchone()
                    if result:
                        tag_id = result[0]
                    else:
                        cursor.execute("INSERT INTO tags (name) VALUES (%s)", (tag,))
                        tag_id = cursor.lastrowid
                    cursor.execute("INSERT IGNORE INTO book_tags (book_id, tag_id) VALUES (%s, %s)", (book_id, tag_id))

        except Exception as e:
            print(f"\n 出错了 (ID: {doc.get('id')}): {str(e)}")
            print("出错的原始数据：", doc)

    mysql_conn.commit()
    print(f"\n 迁移成功！共迁移 {len(docs)} 本书")
    cursor.close()
    mysql_conn.close()

if __name__ == "__main__":
    migrate()