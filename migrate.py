import sqlite3
import pymongo
import os
from bson.binary import Binary
from tqdm import tqdm

def migrate():
    sqlite_path = r"/Users/shen/Desktop/bookstore/fe/data/book_lx.db"
    mongo_uri = "mongodb://localhost:27017/"
    
    # 连接数据库
    print("正在连接数据库...")
    sqlite_conn = sqlite3.connect(sqlite_path)
    mongo_client = pymongo.MongoClient(mongo_uri)
    mongo_db = mongo_client["bookstore_lx"]  # 使用新库避免冲突
    mongo_col = mongo_db["books"]
    
    # 清空现有集合
    if "books" in mongo_db.list_collection_names():
        mongo_col.drop()
    # 获取数据总量
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM book")
    total = cursor.fetchone()[0]
    print(f"共发现 {total} 条待迁移数据")
    
    # 迁移数据
    cursor.execute("SELECT * FROM book")
    for row in tqdm(cursor, total=total, desc="迁移进度", unit="book"):
        try:
            # 处理二进制图片数据
            picture = Binary(row[16]) if row[16] else None
            
            # 构建MongoDB文档
            doc = {
                "_id": row[0],  # id作为主键
                "title": row[1],
                "author": row[2],
                "publisher": row[3],
                "details": {
                    "original_title": row[4],
                    "translator": row[5] if row[5] else None,
                    "pub_year": row[6],
                    "pages": row[7],
                    "price": row[8],
                    "currency_unit": row[9],
                    "binding": row[10]
                },
                "isbn": row[11],
                "content": {
                    "author_intro": row[12],
                    "book_intro": row[13],
                    "full_text": row[14] if row[14] else None
                },
                "tags": row[15].split('|') if row[15] else [],
                "picture": picture,
                "source": "book_lx.db"  # 标记数据来源
            }
            mongo_col.insert_one(doc)
        except Exception as e:
            print(f"\n! 迁移失败(ID:{row[0]}): {str(e)}")
    
    # 创建索引
    print("正在创建索引...")
    mongo_col.create_index("title")
    mongo_col.create_index("author")
    mongo_col.create_index("details.price")
    mongo_col.create_index([("tags", 1)])
    
    print(f"\n迁移完成！验证计数: {mongo_col.count_documents({})}")
    print(f"样例文档:\n{mongo_col.find_one()}")

if __name__ == "__main__":
    migrate()