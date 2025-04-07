import pytest
import uuid
import time
from fe.access.auth import Auth
from fe.access.buyer import Buyer
from fe.access.seller import Seller
from fe.access import book
from fe import conf

class TestMyFeature:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.auth = Auth(conf.URL)
        
        uid = str(uuid.uuid1())
        self.seller_id = f"test_seller_{uid}"
        self.buyer_id = f"test_buyer_{uid}"
        self.password = "test_password"
        self.store_id = f"test_store_{uid}"
        self.book_id = f"test_book_{uid}"

        # 注册用户
        assert self.auth.register(self.seller_id, self.password) == 200
        assert self.auth.register(self.buyer_id, self.password) == 200

        # 创建用户对象
        self.seller = Seller(conf.URL, self.seller_id, self.password)
        self.buyer = Buyer(conf.URL, self.buyer_id, self.password)

        # 创建商店
        assert self.seller.create_store(self.store_id) == 200

        # 添加书籍
        self.book = book.Book()
        self.book.id = self.book_id
        self.book.title = "Test Book"
        self.book.author = "Test Author"
        self.book.publisher = "Test Publisher"
        self.book.price = 100
        self.book.currency_unit = "CNY"
        self.book.tags = ["test"]
        self.book.pictures = []

        assert self.seller.add_book(self.store_id, stock_level=10, book_info=self.book) == 200

        # 买家下单 + 支付 + 卖家发货
        code, self.order_id = self.buyer.new_order(self.store_id, [(self.book_id, 1)])
        assert code == 200
        assert self.buyer.add_funds(1000) == 200
        assert self.buyer.payment(self.order_id) == 200

        yield  # 测试正式开始


    def test_ship_and_receive_flow(self):
        """完整的发货-收货流程测试"""
        # ========== 1. 检查订单状态 ==========
        code, status = self.buyer.get_order_status(self.order_id)
        print(f"[1] 订单状态查询: code={code}, status={status}")
        assert code == 200
        assert status == "paid"
        
        # ========== 2. 发货操作 ==========
        try:
            code, _ = self.seller.ship_order(
                seller_id=self.seller_id,
                store_id=self.store_id,
                order_id=self.order_id
            )
        except Exception as e:
            print(f"发货调用失败: {str(e)}")
            print(f"实际传递参数: seller_id={self.seller_id}, store_id={self.store_id}, order_id={self.order_id}")
            raise

        assert code == 200, f"发货失败，code={code}"
        print("[2] 发货成功")
        
        # ========== 3. 验证发货状态 ==========
        code, status = self.buyer.get_order_status(self.order_id)
        print(f"[3] 发货后状态: code={code}, status={status}")
        assert status == "shipped"
        
        # ========== 4. 收货操作 ==========
        code = self.buyer.receive_order(self.order_id)
        assert code[0] == 200, f"收货失败，code={code}"  
        print("[4] 收货成功")
        
        # ========== 5. 验证完成状态 ==========
        code, status = self.buyer.get_order_status(self.order_id)
        print(f"[5] 最终状态: code={code}, status={status}")
        assert status == "completed" 