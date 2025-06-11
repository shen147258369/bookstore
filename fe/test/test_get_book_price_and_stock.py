import pytest
import uuid
import requests

from fe import conf
from fe.access.new_seller import register_new_seller
from fe.access import book
from fe.access.new_buyer import register_new_buyer
from fe.access.auth import Auth

class TestGetBookPriceAndStock:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        # 创建卖家用户
        self.user_id = f"test_get_price_stock_user_{uuid.uuid1()}"
        self.store_id = f"test_get_price_stock_store_{uuid.uuid1()}"
        self.password = self.user_id
        self.seller = register_new_seller(self.user_id, self.password)
        
        # 创建店铺
        code = self.seller.create_store(self.store_id)
        assert code == 200
        
        # 添加书籍
        book_db = book.BookDB(conf.Use_Large_DB)
        self.books = book_db.get_book_info(0, 5)  # 获取5本书
        assert len(self.books) > 0
        
        # 添加书籍到店铺
        for bk in self.books:
            code = self.seller.add_book(self.store_id, 0, bk)
            assert code == 200
        
        # 创建另一个卖家用户
        self.other_user_id = f"test_other_user_{uuid.uuid1()}"
        self.other_password = self.other_user_id
        self.other_seller = register_new_seller(self.other_user_id, self.other_password)
        
        # 创建另一个店铺
        self.other_store_id = f"test_other_store_{uuid.uuid1()}"
        code = self.other_seller.create_store(self.other_store_id)
        assert code == 200
        
        # 添加书籍到另一个店铺（只添加前两本）
        for bk in self.books[:2]:
            code = self.other_seller.add_book(self.other_store_id, 0, bk)
            assert code == 200
        
        yield

    def test_successful_query(self):
        """测试成功获取图书价格和库存"""
        for bk in self.books:
            code, data = self.seller.get_book_price_and_stock(self.store_id, bk.id)
            
            # 验证返回状态码
            assert code == 200, f"预期200，实际返回{code}"
            
            # 验证数据结构
            assert "stock_quantity" in data, "返回数据缺少stock_quantity"
            assert "book_price" in data, "返回数据缺少book_price"
            
            # 验证数据正确性
            assert isinstance(data["stock_quantity"], int), "库存应为整数"
            assert isinstance(data["book_price"], (int, float)), "价格应为数字"
            
            # 验证库存值正确（添加时库存为0）
            assert data["stock_quantity"] == 0, f"预期库存为0，实际为{data['stock_quantity']}"
            
            # 验证价格正确
            assert data["book_price"] == bk.price, f"预期价格{bk.price}，实际{data['book_price']}"

    def test_book_not_in_store(self):
        """测试查询店铺中不存在的图书"""
        # 创建一个不存在的图书ID
        non_exist_book_id = f"non_exist_book_{uuid.uuid1()}"
        
        code, result = self.seller.get_book_price_and_stock(self.store_id, non_exist_book_id)
        
        # 验证返回状态码
        assert code != 200, f"预期非200状态码，实际返回{code}"
        assert code == 404, f"预期404状态码，实际返回{code}"
        
        # 验证错误信息
        assert "not found" in result.lower(), f"错误信息中应包含'not found'，实际为'{result}'"

    def test_store_not_exist(self):
        """测试查询不存在的店铺"""
        # 创建一个不存在的店铺ID
        non_exist_store_id = f"non_exist_store_{uuid.uuid1()}"
        
        code, result = self.seller.get_book_price_and_stock(non_exist_store_id, self.books[0].id)
        
        # 验证返回状态码
        assert code != 200, f"预期非200状态码，实际返回{code}"
        assert code == 404, f"预期404状态码，实际返回{code}"
        
        # 验证错误信息
        assert "not found" in result.lower(), f"错误信息中应包含'not found'，实际为'{result}'"

    def test_after_stock_change(self):
        """测试修改库存后查询结果"""
        # 选择第一本书
        book_id = self.books[0].id
        
        # 初始查询
        code, initial_data = self.seller.get_book_price_and_stock(self.store_id, book_id)
        assert code == 200
        
        # 添加库存
        add_stock_num = 10
        code = self.seller.add_stock_level(
            self.user_id, self.store_id, book_id, add_stock_num
        )
        assert code == 200
        
        # 再次查询
        code, after_add_data = self.seller.get_book_price_and_stock(self.store_id, book_id)
        assert code == 200
        
        # 验证库存增加
        assert after_add_data["stock_quantity"] == initial_data["stock_quantity"] + add_stock_num, \
            f"库存应为 {initial_data['stock_quantity'] + add_stock_num}，实际为 {after_add_data['stock_quantity']}"
        
        # 减少库存
        decrease_stock_num = -3
        code = self.seller.add_stock_level(
            self.user_id, self.store_id, book_id, decrease_stock_num
        )
        assert code == 200
        
        # 再次查询
        code, final_data = self.seller.get_book_price_and_stock(self.store_id, book_id)
        assert code == 200
        
        # 验证库存减少
        assert final_data["stock_quantity"] == after_add_data["stock_quantity"] + decrease_stock_num, \
            f"库存应为 {after_add_data['stock_quantity'] + decrease_stock_num}，实际为 {final_data['stock_quantity']}"

    def test_after_price_change(self):
        """测试修改价格后查询结果"""
        # 选择第一本书
        book_id = self.books[0].id
        
        # 初始查询
        code, initial_data = self.seller.get_book_price_and_stock(self.store_id, book_id)
        assert code == 200
        
        # 修改价格
        new_price = initial_data["book_price"] + 50
        code = self.seller.change_book_price(self.store_id, book_id, new_price)
        assert code == 200
        
        # 再次查询
        code, after_change_data = self.seller.get_book_price_and_stock(self.store_id, book_id)
        assert code == 200
        
        # 验证价格变化
        assert after_change_data["book_price"] == new_price, \
            f"价格应为 {new_price}，实际为 {after_change_data['book_price']}"
        
        # 验证库存未变化
        assert after_change_data["stock_quantity"] == initial_data["stock_quantity"], \
            "价格变化不应影响库存"

    def test_unauthorized_user(self):
        """测试非卖家用户查询"""
        # 创建普通用户（非卖家）
        buyer_id = f"test_buyer_{uuid.uuid1()}"
        buyer_password = buyer_id
        buyer = register_new_buyer(buyer_id, buyer_password)
        
        # 尝试查询 - 使用API直接调用
        book_id = self.books[0].id
        store_id = self.store_id
        
        # 获取买家token
        auth = Auth(conf.URL)
        code, token = auth.login(buyer_id, buyer_password, "test_terminal")
        assert code == 200
        
        # 构建查询参数
        params = {
            "store_id": store_id,
            "book_id": book_id,
        }
        url = conf.URL + "/seller/get_book_price_and_stock"
        headers = {"token": token}
        
        # 发送请求
        r = requests.get(url, headers=headers, params=params)
        code = r.status_code
        
        # 验证返回状态码
        assert code == 200, "买家用户应该能查询图书价格和库存"
        
        # 验证返回数据
        data = r.json()
        assert "stock_quantity" in data, "返回数据缺少stock_quantity"
        assert "book_price" in data, "返回数据缺少book_price"