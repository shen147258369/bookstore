import os
import random
import base64
import simplejson as json
import mysql.connector
from typing import List

class Book:
    id: str
    title: str
    author: str
    publisher: str
    original_title: str
    translator: str
    pub_year: str
    pages: int
    price: int
    currency_unit: str
    binding: str
    isbn: str
    author_intro: str
    book_intro: str
    content: str
    tags: List[str]
    pictures: List[bytes]

    def __init__(self):
        self.tags = []
        self.pictures = []

class BookDB:
    def __init__(self, large: bool = False):
        # MySQL connection setup
        self.conn = mysql.connector.connect(
            host="localhost", 
            user="stu",
            password="123456",
            database="bookstore_lx"
        )
        self.cursor = self.conn.cursor(dictionary=True)  # Use dictionary cursor to get results as dicts

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def get_book_count(self) -> int:
        self.cursor.execute("SELECT COUNT(*) as count FROM books")
        result = self.cursor.fetchone()
        return result['count']

    def get_book_info(self, start: int, size: int) -> List[Book]:
        books = []
        
        # Get books with pagination
        self.cursor.execute("""
            SELECT * FROM books 
            ORDER BY id 
            LIMIT %s OFFSET %s
        """, (size, start))
        
        for row in self.cursor.fetchall():
            book = Book()
            book.id = row.get("id")
            book.title = row.get("title")
            book.author = row.get("author")
            book.publisher = row.get("publisher")
            book.original_title = row.get("original_title")
            book.translator = row.get("translator")
            book.pub_year = row.get("pub_year")
            book.pages = row.get("pages")
            book.price = row.get("price")
            book.currency_unit = row.get("currency_unit")
            book.binding = row.get("binding")
            book.isbn = row.get("isbn")
            book.author_intro = row.get("author_intro")
            book.book_intro = row.get("book_intro")
            book.content = row.get("content")
            
            # Get tags for this book
            self.cursor.execute("""
                SELECT t.name FROM tags t
                JOIN book_tags bt ON t.id = bt.tag_id
                WHERE bt.book_id = %s
            """, (int(book.id),))  # 确保查询时使用整数类型
            tags = [tag['name'] for tag in self.cursor.fetchall()]
            book.tags = tags
            
            # Handle pictures
            pictures = row.get("pictures")
            if pictures:
                encode_str = base64.b64encode(pictures).decode("utf-8")
                book.pictures.append(encode_str)
            
            books.append(book)
        
        return books