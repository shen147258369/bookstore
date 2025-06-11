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

    def test_reduce_order_item_no_error(self):
        # 选择一本已下单的书
        first_book: Book = self.buy_book_info_list[0][0]
        book_id = first_book.id

        # 正常调用减少商品
        code, _ = self.buyer.reduce_order_item( self.order_id, book_id, 1)
        assert code == 200, f"Expected success, got code {code}"

        # 再次调用减少，即使可能为 0 或负数，仅测试是否报错
        code, _ = self.buyer.reduce_order_item(self.order_id, book_id, 4)
        assert code == 200 or code == 520, f"Expected success or quantity exceed error, got code {code}"

    def test_reduce_order_item_unpaid_order(self):
        """订单状态为已支付时，不能减少商品"""
        first_book: Book = self.buy_book_info_list[0][0]
        book_id = first_book.id

        # 支付订单
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.buyer.payment(self.order_id)
        assert code == 200

        code, _ = self.buyer.reduce_order_item(self.order_id, book_id, 1)
        assert code != 200
        
    def test_reduce_order_item_invalid_order_id(self):
        """尝试操作一个不存在的订单"""
        first_book: Book = self.buy_book_info_list[0][0]
        book_id = first_book.id
        invalid_order_id = "invalid_order_id"

        code, _ = self.buyer.reduce_order_item(invalid_order_id, book_id, 1)
        assert code != 200

    def test_reduce_order_item_by_confuse_user(self):
        """非订单用户尝试减少商品"""
        first_book: Book = self.buy_book_info_list[0][0]
        book_id = first_book.id

        code, _ = self.confuse_buyer.reduce_order_item(self.order_id, book_id, 1)
        assert code != 200

    def test_reduce_order_item_exceed_quantity(self):
        """减少的数量超过当前库存"""
        first_book: Book = self.buy_book_info_list[0][0]
        book_id = first_book.id

        code, _ = self.buyer.reduce_order_item(self.order_id, book_id, 999)
        assert code == 520