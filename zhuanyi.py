import sqlite3
import pymongo
import os
from bson.binary import Binary
from tqdm import tqdm

def migrate():
    sqlite_path = r"/Users/shen/Desktop/bookstore-main/fe/data/book_lx.db"
    mongo_uri = "mongodb://localhost:27017/"
    
    print("正在连接数据库...")
    sqlite_conn = sqlite3.connect(sqlite_path)
    mongo_client = pymongo.MongoClient(mongo_uri)
    mongo_db = mongo_client["bookstore_lx"]
    mongo_col = mongo_db["books"]
    # 清空现有集合
    if "books" in mongo_db.list_collection_names():
        mongo_col.drop()

    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM book")
    total = cursor.fetchone()[0]
    print(f"共发现 {total} 条待迁移数据")

    cursor.execute("SELECT * FROM book")
    for row in tqdm(cursor, total=total, desc="迁移进度", unit="book"):
        try:
            pictures = [Binary(row[16])] if row[16] else []
            tags = row[15].split('|') if row[15] else []

            doc = {
                "id": row[0], 
                "title": row[1],
                "author": row[2],
                "publisher": row[3],
                "original_title": row[4],
                "translator": row[5] if row[5] else None,
                "pub_year": row[6],
                "pages": row[7],
                "price": row[8],
                "currency_unit": row[9],
                "binding": row[10],
                "isbn": row[11],
                "author_intro": row[12],
                "book_intro": row[13],
                "content": row[14] if row[14] else None,
                "tags": tags,
                "pictures": pictures, 
            }
            mongo_col.insert_one(doc)
        except Exception as e:
            print(f"\n! 迁移失败(ID:{row[0]}): {str(e)}")
    
    print("正在创建索引...")
    mongo_col.create_index("title")
    mongo_col.create_index("author")
    mongo_col.create_index("price")
    mongo_col.create_index([("tags", 1)])
    
    print(f"\n迁移完成,验证计数: {mongo_col.count_documents({})}")
    print(f"样例文档:\n{mongo_col.find_one()}")

if __name__ == "__main__":
    migrate()