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
        self.seller_id = "test_receive_seller_id_{}".format(str(uuid.uuid1()))
        self.store_id = "test_receive_store_id_{}".format(str(uuid.uuid1()))
        self.buyer_id = "test_receive_buyer_id_{}".format(str(uuid.uuid1()))
        self.password = self.buyer_id
        print(f"Generated buyer_id: {self.buyer_id}")
        print(f"Generated seller_id: {self.seller_id}")
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

    def test_receive_order_ok(self):
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.buyer.payment(self.order_id)
        assert code == 200
        code, message = self.seller.ship_order(self.seller_id, self.store_id, self.order_id)
        assert code == 200
        
        code, message = self.buyer.receive_order(self.buyer_id, self.order_id)
        assert code == 200

    def test_receive_order_non_exist_order_id(self):
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.buyer.payment(self.order_id)
        assert code == 200
        code, message = self.seller.ship_order(self.seller_id, self.store_id, self.order_id)
        assert code == 200

        non_exist_order_id = self.order_id + "_x"
        code, message = self.buyer.receive_order(self.buyer_id, non_exist_order_id)
        assert code != 200

    def test_receive_order_authorization_fail(self):
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.buyer.payment(self.order_id)
        assert code == 200
        code, message = self.seller.ship_order(self.seller_id, self.store_id, self.order_id)
        assert code == 200
        
        wrong_buyer_id = self.buyer_id + "_x"
        code, message = self.buyer.receive_order(wrong_buyer_id, self.order_id)
        assert code != 200

    def test_receive_order_invalid_order_status(self):
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.buyer.payment(self.order_id)
        assert code == 200

        code, message = self.buyer.receive_order(self.buyer_id, self.order_id)
        assert code != 200
