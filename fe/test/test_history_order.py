import pytest

from fe.access.buyer import Buyer
from fe.access.seller import Seller
from fe.test.gen_book_data import GenBook
from fe.access.new_buyer import register_new_buyer
from fe.access.new_seller import register_new_seller
from fe.access.book import Book
import uuid
from fe import conf

class TestReceiveOrder:
    seller_id: str
    buyer_id: str
    order_id: str
    store_id: str
    book_info_list: [Book]
    total_price: int
    buyer: Buyer
    seller: Seller

    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.seller_id = "test_history_seller_id_{}".format(str(uuid.uuid1()))
        self.store_id = "test_history_store_id_{}".format(str(uuid.uuid1()))
        self.buyer_id = "test_history_buyer_id_{}".format(str(uuid.uuid1()))
        self.password = self.buyer_id
        gen_book = GenBook(self.seller_id, self.store_id)
        ok, buy_book_id_list = gen_book.gen(
            non_exist_book_id=False, low_stock_level=False, max_book_count=5
        )
        self.book_info_list = gen_book.buy_book_info_list
        assert ok
        b = register_new_buyer(self.buyer_id, self.password)
        self.buyer = b
        code, self.order_id = b.new_order(self.store_id, buy_book_id_list)
        assert code == 200
        self.total_price = 0
        for item in self.book_info_list:
            book: Book = item[0]
            num = item[1]
            if book.price is None:
                continue
            else:
                self.total_price = self.total_price + book.price * num
        self.seller = Seller(conf.URL, self.seller_id, self.seller_id)
        yield

    def test_get_order_history_unpaid(self):
        code, message, orders = self.buyer.get_order_history()
        assert code == 200
        assert isinstance(orders, list)
        
    def test_get_order_history_paid(self):
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.buyer.payment(self.order_id)
        assert code == 200

        code, message, orders = self.buyer.get_order_history()
        assert code == 200
        assert isinstance(orders, list)

    def test_get_order_history_cancelled(self):
        code = self.buyer.cancel_order(self.order_id)
        assert code[0] == 200

        code, message, orders = self.buyer.get_order_history()
        assert code == 200

        found = False
        for order in orders:
            if order["order_id"] == self.order_id:
                found = True
                assert order["status"] == "cancelled"  
        assert found




