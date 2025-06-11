import pytest
from fe.access.new_seller import register_new_seller
import uuid
from fe import conf
from fe.access import book

class TestCreateStore:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.user_id = "test_create_store_user_{}".format(str(uuid.uuid1()))
        self.store_id = "test_create_store_store_{}".format(str(uuid.uuid1()))
        self.password = self.user_id
        yield

    def test_ok(self):
        self.seller = register_new_seller(self.user_id, self.password)
        code = self.seller.create_store(self.store_id)
        assert code == 200

    def test_error_exist_store_id(self):
        self.seller = register_new_seller(self.user_id, self.password)
        code = self.seller.create_store(self.store_id)
        assert code == 200

        code = self.seller.create_store(self.store_id)
        assert code != 200


class TestChangeStoreName:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        # 创建店主用户
        self.owner_id = f"test_owner_{uuid.uuid1()}"
        self.owner_password = self.owner_id
        self.owner = register_new_seller(self.owner_id, self.owner_password)

        # 创建店铺
        self.store_id = f"test_store_{uuid.uuid1()}"
        code = self.owner.create_store(self.store_id)
        assert code == 200

        # 添加书籍
        book_db = book.BookDB(conf.Use_Large_DB)
        self.books = book_db.get_book_info(0, 1)
        assert len(self.books) > 0
        self.book = self.books[0]
        code = self.owner.add_book(self.store_id, 0, self.book)
        assert code == 200

        # 创建另一个用户用于测试权限
        self.other_user_id = f"test_other_{uuid.uuid1()}"
        self.other_password = self.other_user_id
        self.other_seller = register_new_seller(self.other_user_id, self.other_password)

        yield

    def test_authorization_failure(self):
        """测试非店主用户修改店铺名称"""
        new_name = "New Store Name"
        code = self.other_seller.change_store_name(self.store_id, new_name)
        assert code != 200

    def test_invalid_store_id(self):
        """测试无效店铺ID"""
        new_name = "New Store Name"
        invalid_store_id = f"invalid_{uuid.uuid1()}"
        code = self.owner.change_store_name(invalid_store_id, new_name)
        assert code != 200

    def test_successful_change(self):
        """测试成功修改店铺名称"""
        new_name = "Awesome Bookstore"
        code = self.owner.change_store_name(self.store_id, new_name)
        assert code == 200

    def test_empty_store_name(self):
        """测试空店铺名称"""
        new_name = ""
        code = self.owner.change_store_name(self.store_id, new_name)
        assert code != 200

    def test_long_store_name(self):
        """测试超长店铺名称"""
        new_name = "A" * 256  # 超过255字符的名称
        code = self.owner.change_store_name(self.store_id, new_name)
        assert code != 200