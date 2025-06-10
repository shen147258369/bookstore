import pytest
import uuid

from fe import conf
from fe.access.new_seller import register_new_seller
from fe.access import book


class TestChangeBookPrice:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.user_id = f"test_change_price_user_{uuid.uuid1()}"
        self.store_id = f"test_change_price_store_{uuid.uuid1()}"
        self.password = self.user_id
        self.seller = register_new_seller(self.user_id, self.password)

        code = self.seller.create_store(self.store_id)
        assert code == 200

        book_db = book.BookDB(conf.Use_Large_DB)
        self.books = book_db.get_book_info(0, 1)
        assert len(self.books) > 0
        self.book = self.books[0]
        code = self.seller.add_book(self.store_id, 0, self.book)
        assert code == 200

        # 创建另一个用户用于测试权限
        self.another_user_id = f"test_another_user_{uuid.uuid1()}"
        self.another_password = self.another_user_id
        self.another_seller = register_new_seller(self.another_user_id, self.another_password)

        yield

    def test_error_user_id(self):
        """测试非店主用户修改价格"""
        new_price = self.book.price + 100
        # 使用另一个用户（非店铺所有者）尝试修改价格
        code = self.another_seller.change_book_price(self.store_id, self.book.id, new_price)
        assert code != 200  # 应返回非200

    def test_error_store_id(self):
        """测试不存在的店铺ID"""
        new_price = self.book.price + 100
        # 使用不存在的店铺ID
        non_exist_store_id = f"non_exist_store_{uuid.uuid1()}"
        code = self.seller.change_book_price(non_exist_store_id, self.book.id, new_price)
        assert code != 200  # 应返回非200

    def test_error_book_id(self):
        """测试不存在的图书ID"""
        new_price = self.book.price + 100
        # 使用不存在的图书ID
        non_exist_book_id = f"non_exist_book_{uuid.uuid1()}"
        code = self.seller.change_book_price(self.store_id, non_exist_book_id, new_price)
        assert code != 200  # 应返回非200

    def test_ok(self):
        """测试合法修改"""
        new_price = self.book.price + 100
        code = self.seller.change_book_price(self.store_id, self.book.id, new_price)
        assert code == 200