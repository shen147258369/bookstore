import os
import random
import base64
from pymongo import MongoClient
from typing import List

class Book:
    def __init__(self):
        self.id: str = ""
        self.title: str = ""
        self.author: str = ""
        self.publisher: str = ""
        self.original_title: str = ""
        self.translator: str = ""
        self.pub_year: str = ""
        self.pages: int = 0
        self.price: int = 0
        self.currency_unit: str = ""
        self.binding: str = ""
        self.isbn: str = ""
        self.author_intro: str = ""
        self.book_intro: str = ""
        self.content: str = ""
        self.tags: List[str] = []
        self.pictures: List[str] = []

class BookDB:
    def __init__(self, large: bool = False):
        # MongoDB连接配置
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['bookstore']
        self.books_col = self.db['books']
        
        # 初始化索引
        self._create_indexes()

    def _create_indexes(self):
        """创建必要索引"""
        self.books_col.create_index("id", unique=True)
        self.books_col.create_index("title")
        self.books_col.create_index("author")
        self.books_col.create_index([("tags", 1)])

    def get_book_count(self) -> int:
        return self.books_col.count_documents({})

    def get_book_info(self, start: int, size: int) -> List[Book]:
        books = []
        
        # MongoDB分页查询
        cursor = self.books_col.find().skip(start).limit(size)
        
        for doc in cursor:
            book = Book()
            book.id = doc.get("id", "")
            book.title = doc.get("title", "")
            book.author = doc.get("author", "")
            book.publisher = doc.get("publisher", "")
            book.original_title = doc.get("original_title", "")
            book.translator = doc.get("translator", "")
            book.pub_year = doc.get("pub_year", "")
            book.pages = doc.get("pages", 0)
            book.price = doc.get("price", 0)
            book.currency_unit = doc.get("currency_unit", "")
            book.binding = doc.get("binding", "")
            book.isbn = doc.get("isbn", "")
            book.author_intro = doc.get("author_intro", "")
            book.book_intro = doc.get("book_intro", "")
            book.content = doc.get("content", "")
            book.tags = doc.get("tags", [])
            
            # 图片处理
            pictures = doc.get("pictures", [])
            for _ in range(random.randint(0, 9)):
                if pictures:
                    book.pictures.append(random.choice(pictures))
                    
            books.append(book)
        return books