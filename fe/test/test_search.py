import pytest
from fe.access.buyer import Buyer
from fe.test.gen_book_data import GenBook
from fe.access.new_buyer import register_new_buyer
import uuid

class TestSearchBooks:
    seller_id: str
    store_id: str
    buyer_id: str
    password: str
    buy_book_info_list: list  # 包含(Book, stock)元组的列表
    total_price: int
    order_id: str
    buyer: Buyer

    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.seller_id = "test_search_seller_id_{}".format(str(uuid.uuid1()))
        self.store_id = "test_search_store_id_{}".format(str(uuid.uuid1()))
        self.buyer_id = "test_search_buyer_id_{}".format(str(uuid.uuid1()))
        self.password = self.seller_id
        gen_book = GenBook(self.seller_id, self.store_id)
        ok, buy_book_id_list = gen_book.gen(
            non_exist_book_id=False, low_stock_level=False, max_book_count=5
        )
        self.buy_book_info_list = gen_book.buy_book_info_list  # 这是(Book, stock)元组列表
        assert ok
        b = register_new_buyer(self.buyer_id, self.password)
        self.buyer = b
        code, self.order_id = b.new_order(self.store_id, buy_book_id_list)
        assert code == 200
        yield

    def test_search_books_by_title(self):
        book_obj = self.buy_book_info_list[0][0]
        query = book_obj.title
        search_field = 'title'
        
        code, result = self.buyer.search_books(
            query=query,
            search_field=search_field,
            store_id=self.store_id,
            page=1,
            per_page=10
        )
        
        assert code == 200
        assert len(result['books']) > 0, f"No books found when searching for title: {query}"
        assert any(book['title'] == query for book in result['books']), \
            f"Expected book with title '{query}' not found in results: {result['books']}"
        
    def test_search_books_by_tags(self):
        book_obj = self.buy_book_info_list[0][0]
        assert len(book_obj.tags) > 0, "The book has no tags, cannot test tag search"
        query = book_obj.tags[0] 
        search_field = 'tags'

        code, result = self.buyer.search_books(
            query=query,
            search_field=search_field,
            store_id=self.store_id,
            page=1,
            per_page=10
        )
        assert code == 200, f"Expected status code 200, but got {code}"
        assert len(result['books']) > 0, f"No books found when searching for tag: {query}"
        assert any(query in book['tags'] for book in result['books']), \
            f"Expected book with tag '{query}' not found in results: {result['books']}"

    def test_search_books_by_publisher(self):
        book_obj = self.buy_book_info_list[0][0]
        query = book_obj.publisher
        search_field = 'publisher'

        code, result = self.buyer.search_books(
            query=query,
            search_field=search_field,
            store_id=self.store_id,
            page=1,
            per_page=10
        )

        assert code == 200, f"Expected status code 200, but got {code}"
        assert len(result['books']) > 0, f"No books found when searching for publisher: {query}"
        assert any(book['publisher'] == query for book in result['books']), \
            f"Expected book with publisher '{query}' not found in results: {result['books']}"

    def test_search_books_global(self):
        book_obj = self.buy_book_info_list[0][0]
        
        query_words = book_obj.title.split()[:2]
        query = ' '.join(query_words)
        
        code, result = self.buyer.search_books(
            query=query,
            search_field='all',  
            store_id=None,      
            page=1,
            per_page=10
        )
        
        assert code == 200, f"Expected status code 200, but got {code}"
        assert len(result['books']) > 0, f"No books found when searching globally for: {query}"
        
        assert any(
            query.lower() in book['title'].lower() or 
            query.lower() in book['author'].lower() or
            query.lower() in book['publisher'].lower() or
            query.lower() in book['content'].lower() or
            any(query.lower() in tag.lower() for tag in book.get('tags', []))
            for book in result['books']
        ), f"Expected books matching '{query}' not found in global results"
        
    def test_content_search(self):
        book_obj = self.buy_book_info_list[0][0]
        content_list = book_obj.content
        assert len(content_list) > 0, "书籍内容为空"
        query = content_list

        code, result = self.buyer.search_books(
            query=query,
            search_field='content',
            store_id=None,
            page=1,
            per_page=10
        )
        assert code == 200
        assert len(result['books']) > 0, f"未找到内容匹配项: {query}"