import pytest

from fe.test.gen_book_data import GenBook
from fe.access.new_buyer import register_new_buyer
import uuid


class TestNewOrder:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.seller_id = "test_new_order_seller_id_{}".format(str(uuid.uuid1()))
        self.store_id = "test_new_order_store_id_{}".format(str(uuid.uuid1()))
        self.buyer_id = "test_new_order_buyer_id_{}".format(str(uuid.uuid1()))
        self.password = self.seller_id
        self.buyer = register_new_buyer(self.buyer_id, self.password)
        self.gen_book = GenBook(self.seller_id, self.store_id)
        yield

    def test_non_exist_book_id(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=True, low_stock_level=False
        )
        assert ok
        code, _ = self.buyer.new_order(self.store_id, buy_book_id_list)
        print (code)
        assert code != 200

    def test_low_stock_level(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=True
        )
        assert ok
        code, _ = self.buyer.new_order(self.store_id, buy_book_id_list)
        print(code)
        assert code != 200

    def test_ok(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=False
        )
        assert ok
        code, _ = self.buyer.new_order(self.store_id, buy_book_id_list)
        assert code == 200

    def test_non_exist_user_id(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=False
        )
        assert ok
        self.buyer.user_id = self.buyer.user_id + "_x"
        code, _ = self.buyer.new_order(self.store_id, buy_book_id_list)
        assert code != 200

    def test_non_exist_store_id(self):
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=False
        )
        assert ok
        code, _ = self.buyer.new_order(self.store_id + "_x", buy_book_id_list)
        assert code != 200

    def test_append_to_existing_order(self):
        # Step 1: 第一次下单，添加两本书（手动构造列表）
        ok, book_info = self.gen_book.gen(non_exist_book_id=False, low_stock_level=False)
        assert ok
        book_id1, count1 = book_info[0][0], 1  # 取第一本书，数量为1

        ok, book_info2 = self.gen_book.gen(non_exist_book_id=False, low_stock_level=False)
        assert ok
        book_id2, count2 = book_info2[0][0], 1  # 再取一本，数量为1

        buy_book_id_list_1 = [(book_id1, count1), (book_id2, count2)]

        code, order_id_1 = self.buyer.new_order(self.store_id, buy_book_id_list_1)
        assert code == 200

        # Step 2: 再次下单，添加另外两本书（同样手动构造）
        ok, book_info3 = self.gen_book.gen(non_exist_book_id=False, low_stock_level=False)
        assert ok
        book_id3, count3 = book_info3[0][0], 1

        ok, book_info4 = self.gen_book.gen(non_exist_book_id=False, low_stock_level=False)
        assert ok
        book_id4, count4 = book_info4[0][0], 1

        buy_book_id_list_2 = [(book_id3, count3), (book_id4, count4)]

        code, order_id_2 = self.buyer.new_order(self.store_id, buy_book_id_list_2)
        assert code == 200

        # Step 3: 检查是否是同一个订单
        assert order_id_1 == order_id_2
        
    def test_cannot_append_to_paid_order(self):
        # Step 1: 第一次下单
        ok, buy_book_id_list = self.gen_book.gen(non_exist_book_id=False, low_stock_level=False)
        assert ok
        code, order_id_1 = self.buyer.new_order(self.store_id, buy_book_id_list)
        assert code == 200

        code, message = self.buyer.cancel_order(order_id_1)
        assert code == 200

        # Step 3: 再次下单
        ok, another_book_list = self.gen_book.gen(non_exist_book_id=False, low_stock_level=False)
        assert ok
        code, order_id_2 = self.buyer.new_order(self.store_id, another_book_list)
        assert code == 200

        # Step 4: 确保是两个不同的订单
        assert order_id_1 != order_id_2
