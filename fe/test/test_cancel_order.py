import pytest
import uuid

from fe import conf
from fe.access.buyer import Buyer
from fe.access.seller import Seller
from fe.access.book import Book
from fe.test.gen_book_data import GenBook
from fe.access.new_buyer import register_new_buyer
from fe.access.new_seller import register_new_seller

class TestSend:
    seller_id: str
    confuse_seller_id: str
    buyer_id: str
    confuse_buyer_id: str
    password: str
    store_id: str
    buyer: Buyer
    confuse_buyer: Buyer
    seller: Seller
    confuse_seller: Seller
    buy_book_info_list: [Book]
    total_price: int
    order_id: str

    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.seller_id = "test_cancelid_{}".format(str(uuid.uuid1()))
        self.confuse_seller_id = "test_send_confuse_cancelid_{}".format(str(uuid.uuid1()))
        self.buyer_id = "test_cancel_buyer_id_{}".format(str(uuid.uuid1()))
        self.confuse_buyer_id = "test_confuse_buyer_id_{}".format(str(uuid.uuid1()))
        self.store_id = "test_cancel_store_id_{}".format(str(uuid.uuid1()))
        self.password = self.seller_id

        gen_book = GenBook(self.seller_id, self.store_id)
        ok, buy_book_id_list = gen_book.gen(
            non_exist_book_id=False, low_stock_level=False, max_book_count=5
        )
        self.buy_book_info_list = gen_book.buy_book_info_list
        assert ok

        self.buyer = register_new_buyer(self.buyer_id, self.password)

        self.confuse_buyer = register_new_buyer(self.confuse_buyer_id, self.password)

        code, self.order_id = self.buyer.new_order(self.store_id, buy_book_id_list)
        assert code == 200

        self.total_price = 0
        for item in self.buy_book_info_list:
            book: Book = item[0]
            num = item[1]
            if book.price is not None:
                self.total_price += book.price * num

        self.seller = Seller(conf.URL, self.seller_id, self.seller_id)
        self.confuse_seller = register_new_seller(self.confuse_seller_id, self.confuse_seller_id)

        yield

    def test_cancel_order_ok(self):
        code, message = self.buyer.cancel_order(self.order_id)
        assert code == 200
        code, message = self.buyer.cancel_order(self.order_id)
        assert code != 200

    def test_cancel_order_already_paid(self):
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.buyer.payment(self.order_id)
        assert code == 200

        code, message = self.buyer.cancel_order(self.order_id)
        assert code != 200
        
    def test_cancel_order_invalid_order_id(self):
        invalid_order_id = "invalid_order_id"
        code, message = self.buyer.cancel_order(invalid_order_id)
        assert code != 200

    def test_confuse_buyer_cannot_cancel_order(self):
            code = self.confuse_buyer.cancel_order(self.order_id)
            assert code != 200  
            
    def test_cancel_order_already_cancelled(self):
        code, message = self.buyer.cancel_order(self.order_id)
        assert code == 200
        code, message = self.buyer.cancel_order(self.order_id)
        assert code != 200
