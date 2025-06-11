import pytest

from fe import conf
from fe.access.new_seller import register_new_seller
from fe.access import book
import uuid


class TestAddStockLevel:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.user_id = "test_add_book_stock_level1_user_{}".format(str(uuid.uuid1()))
        self.store_id = "test_add_book_stock_level1_store_{}".format(str(uuid.uuid1()))
        self.password = self.user_id
        self.seller = register_new_seller(self.user_id, self.password)

        code = self.seller.create_store(self.store_id)
        assert code == 200
        book_db = book.BookDB(conf.Use_Large_DB)
        self.books = book_db.get_book_info(0, 5)
        for bk in self.books:
            code = self.seller.add_book(self.store_id, 0, bk)
            assert code == 200
        yield

    def test_ok(self):
        for b in self.books:
            book_id = b.id

            # 获取当前库存数量
            code, data = self.seller.get_book_price_and_stock(self.store_id, book_id)
            assert code == 200
            original_stock = data["stock_quantity"]

            # 添加库存
            add_stock_num = 10
            code = self.seller.add_stock_level(self.user_id, self.store_id, book_id, add_stock_num)
            assert code == 200

            # 再次查询库存，验证是否正确增加
            code, data = self.seller.get_book_price_and_stock(self.store_id, book_id)
            assert code == 200
            new_stock = data["stock_quantity"]
            assert new_stock == original_stock + add_stock_num, \
                f"库存应为 {original_stock + add_stock_num}，但实际为 {new_stock}"

    def test_error_user_id(self):
        for b in self.books:
            book_id = b.id
            code = self.seller.add_stock_level(
                self.user_id + "_x", self.store_id, book_id, 10
            )
            assert code != 200

    def test_error_store_id(self):
        for b in self.books:
            book_id = b.id
            code = self.seller.add_stock_level(
                self.user_id, self.store_id + "_x", book_id, 10
            )
            assert code != 200

    def test_error_book_id(self):
        for b in self.books:
            book_id = b.id
            code = self.seller.add_stock_level(
                self.user_id, self.store_id, book_id + "_x", 10
            )
            assert code != 200



    def test_ok_decrease_stock(self):
        for b in self.books:
            book_id = b.id

            # 获取当前库存数量（初始）
            code, data = self.seller.get_book_price_and_stock(self.store_id, book_id)
            assert code == 200
            original_stock = data["stock_quantity"]

            # 添加一些库存
            add_stock_num = 10
            code = self.seller.add_stock_level(self.user_id, self.store_id, book_id, add_stock_num)
            assert code == 200

            # 再次获取库存，确认增加了
            code, data = self.seller.get_book_price_and_stock(self.store_id, book_id)
            assert code == 200
            after_add_stock = data["stock_quantity"]
            assert after_add_stock == original_stock + add_stock_num, \
                f"库存应为 {original_stock + add_stock_num}，但实际为 {after_add_stock}"

            # 减少库存
            decrease_stock_num = -3
            code = self.seller.add_stock_level(self.user_id, self.store_id, book_id, decrease_stock_num)
            assert code == 200

            # 最后再次获取库存，确认减少了
            code, data = self.seller.get_book_price_and_stock(self.store_id, book_id)
            assert code == 200
            final_stock = data["stock_quantity"]
            expected_stock = after_add_stock + decrease_stock_num  # 因为 decrease_stock_num 是负数
            assert final_stock == expected_stock, \
                f"库存应为 {expected_stock}，但实际为 {final_stock}"
        
    def test_decrease_below_zero(self):
        for b in self.books:
            book_id = b.id

            # 获取当前库存数量（初始）
            code, data = self.seller.get_book_price_and_stock(self.store_id, book_id)
            assert code == 200
            original_stock = data["stock_quantity"]

            # 添加一些库存
            add_stock_num = 5
            code = self.seller.add_stock_level(self.user_id, self.store_id, book_id, add_stock_num)
            assert code == 200

            # 再次获取库存，确认增加了
            code, data = self.seller.get_book_price_and_stock(self.store_id, book_id)
            assert code == 200
            after_add_stock = data["stock_quantity"]
            assert after_add_stock == original_stock + add_stock_num, \
                f"库存应为 {original_stock + add_stock_num}，但实际为 {after_add_stock}"

            # 尝试减少超过库存的数量（-10）
            decrease_stock_num = -10
            code = self.seller.add_stock_level(self.user_id, self.store_id, book_id, decrease_stock_num)
            assert code == 200

            # 最后再次获取库存，确认被限制为 0
            code, data = self.seller.get_book_price_and_stock(self.store_id, book_id)
            assert code == 200
            final_stock = data["stock_quantity"]
            expected_stock = 0 
            assert final_stock == expected_stock, \
                f"库存应为 {expected_stock}，但实际为 {final_stock}"
            