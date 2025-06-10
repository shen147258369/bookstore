import requests
from urllib.parse import urljoin
from fe.access import book
from fe.access.auth import Auth


class Seller:
    def __init__(self, url_prefix, seller_id: str, password: str):
        self.url_prefix = urljoin(url_prefix, "seller/")
        self.seller_id = seller_id
        self.password = password
        self.terminal = "my terminal"
        self.auth = Auth(url_prefix)
        code, self.token = self.auth.login(self.seller_id, self.password, self.terminal)
        assert code == 200

    def create_store(self, store_id):
        json = {
            "user_id": self.seller_id,
            "store_id": store_id,
        }
        # print(simplejson.dumps(json))
        url = urljoin(self.url_prefix, "create_store")
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json)
        return r.status_code

    def add_book(self, store_id: str, stock_level: int, book_info: book.Book) -> int:
        json = {
            "user_id": self.seller_id,
            "store_id": store_id,
            "book_info": book_info.__dict__,
            "stock_level": stock_level,
        }
        # print(simplejson.dumps(json))
        url = urljoin(self.url_prefix, "add_book")
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json)
        return r.status_code

    def add_stock_level(
        self, seller_id: str, store_id: str, book_id: str, add_stock_num: int
    ) -> int:
        json = {
            "user_id": seller_id,
            "store_id": store_id,
            "book_id": book_id,
            "add_stock_level": add_stock_num,
        }
        # print(simplejson.dumps(json))
        url = urljoin(self.url_prefix, "add_stock_level")
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json)
        return r.status_code


    def ship_order(self, seller_id: str, store_id: str, order_id: str) -> int:
        """
        卖家发货接口
        :param seller_id: 卖家ID
        :param store_id: 店铺ID
        :param order_id: 订单ID
        :return: HTTP状态码 (200表示成功)
        """
        json = {
            "user_id": seller_id,
            "store_id": store_id,
            "order_id": order_id
        }
        url = urljoin(self.url_prefix, "ship_order")
        headers = {"token": self.token}
        try:
            r = requests.post(url, headers=headers, json=json)
            if r.status_code == 200:
                return r.status_code, "发货成功"
            else:
                return r.status_code, "发货失败"
        except requests.exceptions.RequestException as e:
            return 500, f"请求失败: {str(e)}"

    def get_order_status(self, order_id: str) -> (int, str):
        """
        获取订单状态（可选，用于测试验证）
        :param order_id: 订单ID
        :return: (HTTP状态码, 订单状态字符串)
        """
        params = {
            "user_id": self.seller_id,
            "order_id": order_id
        }
        url = urljoin(self.url_prefix, "order_status")
        headers = {"token": self.token}
        r = requests.get(url, headers=headers, params=params)
        if r.status_code != 200:
            return r.status_code, ""
        return r.status_code, r.json().get("status")
    
    def change_book_price(self, store_id: str, book_id: str, new_price: int) -> int:
        json = {
            "user_id": self.seller_id,
            "store_id": store_id,
            "book_id": book_id,
            "new_price": new_price,
        }
        url = urljoin(self.url_prefix, "change_book_price") 
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json) 
        return r.status_code
