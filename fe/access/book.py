import os
import random
import base64
import simplejson as json
import pymongo

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
    tags: [str]
    pictures: [bytes]

    def __init__(self):
        self.tags = []
        self.pictures = []


class BookDB:
    def __init__(self, large: bool = False):
        parent_path = os.path.dirname(os.path.dirname(__file__))
        self.db_s = os.path.join(parent_path, "data/book.db")
        self.db_l = os.path.join(parent_path, "data/book_lx.db")
        if large:
            self.book_db = self.db_l
        else:
            self.book_db = self.db_s
        # 连接到 MongoDB
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["bookstore_lx"] 
        self.collection = self.db["books"]

    def get_book_count(self):
        return self.collection.count_documents({})

    def get_book_info(self, start, size) -> [Book]:
        books = []
        cursor = self.collection.find().sort("id").skip(start).limit(size)
        for doc in cursor:
            book = Book()
            book.id = doc.get("id")
            book.title = doc.get("title")
            book.author = doc.get("author")
            book.publisher = doc.get("publisher")
            book.original_title = doc.get("original_title")
            book.translator = doc.get("translator")
            book.pub_year = doc.get("pub_year")
            book.pages = doc.get("pages")
            book.price = doc.get("price")
            book.currency_unit = doc.get("currency_unit")
            book.binding = doc.get("binding")
            book.isbn = doc.get("isbn")
            book.author_intro = doc.get("author_intro")
            book.book_intro = doc.get("book_intro")
            book.content = doc.get("content")
            tags = doc.get("tags")
            picture = doc.get("picture")

            if tags:
                    if len(tags) == 1 and isinstance(tags[0], str):
                        book.tags.extend(tag.strip() for tag in tags[0].split("\n") if tag.strip())

            if picture:
                for _ in range(0, random.randint(0, 9)):
                    encode_str = base64.b64encode(picture).decode("utf-8")
                    book.pictures.append(encode_str)
            books.append(book)
        return books