import pymongo
import mysql.connector
from tqdm import tqdm
import re

def migrate():
    # MongoDB 连接
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["bookstore_lx"]
    mongo_col = mongo_db["books"]

    # MySQL 连接（修改你的密码）
    mysql_conn = mysql.connector.connect(
    host="localhost", 
    unix_socket="/tmp/mysql.sock",
    user="root",
    password="ssy0729",
    auth_plugin='mysql_native_password'  # 添加插件指定
    )
    cursor = mysql_conn.cursor()

    # 创建数据库和表
    cursor.execute("CREATE DATABASE IF NOT EXISTS bookstore_lx")
    cursor.execute("USE bookstore_lx")

    cursor.execute("DROP TABLE IF EXISTS book_tags")
    cursor.execute("DROP TABLE IF EXISTS tags")
    cursor.execute("DROP TABLE IF EXISTS books")

    # 创建表结构
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INT PRIMARY KEY,
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
            author_intro TEXT,
            book_intro TEXT
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
            book_id INT,
            tag_id INT,
            PRIMARY KEY (book_id, tag_id),
            FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
        )
    """)

    # 清空表（可选）
    cursor.execute("DELETE FROM book_tags")
    cursor.execute("DELETE FROM tags")
    cursor.execute("DELETE FROM books")
    mysql_conn.commit()

    # 读取所有书籍记录，排除 content 和 pictures 字段
    docs = list(mongo_col.find({}, {"content": 0, "pictures": 0}))
    print(f"📦 共读取 {len(docs)} 条图书记录，开始迁移...")

    for doc in tqdm(docs, desc="迁移中", unit="本书"):
        try:
            # 插入 books 表
            cursor.execute("""
                INSERT INTO books (
                    id, title, author, publisher, original_title, translator,
                    pub_year, pages, price, currency_unit, binding, isbn,
                    author_intro, book_intro
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                doc["id"], doc["title"], doc["author"], doc["publisher"], doc["original_title"],
                doc.get("translator"), doc["pub_year"], doc["pages"], doc["price"],
                doc["currency_unit"], doc["binding"], doc["isbn"],
                doc["author_intro"], doc["book_intro"]
            ))

            # 处理 tags 字段：拆分 \n 和 中文逗号，去空格，去特殊字符，插入数据库
            for raw_tag_str in doc.get("tags", []):
                # 先拆分 \n 和 中文逗号
                tag_candidates = re.split(r'[\n，]', raw_tag_str)
                for tag in tag_candidates:
                    tag = tag.strip()
                    # 去除除字母数字中文空格连字符外的字符（如 * 等）
                    tag = re.sub(r'[^\w\s\u4e00-\u9fff\-]', '', tag)
                    if not tag:
                        continue

                    # 查找或插入 tag
                    cursor.execute("SELECT id FROM tags WHERE name = %s", (tag,))
                    result = cursor.fetchone()
                    if result:
                        tag_id = result[0]
                    else:
                        cursor.execute("INSERT INTO tags (name) VALUES (%s)", (tag,))
                        tag_id = cursor.lastrowid
                    cursor.execute("INSERT IGNORE INTO book_tags (book_id, tag_id) VALUES (%s, %s)", (doc["id"], tag_id))

        except Exception as e:
            print(f"\n❌ 出错了 (ID: {doc.get('id')}): {str(e)}")
            print("    ↪ 出错的原始数据：", doc)

    mysql_conn.commit()
    print(f"\n✅ 迁移成功！共迁移 {len(docs)} 本书")
    cursor.close()
    mysql_conn.close()

if __name__ == "__main__":
    migrate()
