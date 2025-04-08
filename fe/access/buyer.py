import requests
import simplejson
from urllib.parse import urljoin
from fe.access.auth import Auth
from typing import Tuple, List
from typing import Any


class Buyer:
    def __init__(self, url_prefix, user_id, password):
        self.url_prefix = urljoin(url_prefix, "buyer/")
        self.user_id = user_id
        self.password = password
        self.token = ""
        self.terminal = "my terminal"
        self.auth = Auth(url_prefix)
        code, self.token = self.auth.login(self.user_id, self.password, self.terminal)
        assert code == 200

    def new_order(self, store_id: str, book_id_and_count: [(str, int)]) -> (int, str):
        books = []
        for id_count_pair in book_id_and_count:
            books.append({"id": id_count_pair[0], "count": id_count_pair[1]})
        json = {"user_id": self.user_id, "store_id": store_id, "books": books}
        # print(simplejson.dumps(json))
        url = urljoin(self.url_prefix, "new_order")
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json)
        response_json = r.json()
        return r.status_code, response_json.get("order_id")

    def payment(self, order_id: str):
        json = {
            "user_id": self.user_id,
            "password": self.password,
            "order_id": order_id,
        }
        url = urljoin(self.url_prefix, "payment")
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json)
        return r.status_code

    def add_funds(self, add_value: str) -> int:
        json = {
            "user_id": self.user_id,
            "password": self.password,
            "add_value": add_value,
        }
        url = urljoin(self.url_prefix, "add_funds")
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json)
        return r.status_code
    
    def receive_order(self, user_id: str, order_id: str) -> (int, str):
        json = {
            "user_id": user_id,
            "order_id": order_id,
        }
        url = urljoin(self.url_prefix, "receive_order")
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json)
        response_data = r.json()
        message = response_data.get("message", "No message in response")

        return r.status_code, message


    def get_order_status(self, order_id: str) -> (int, str):
            headers = {"token": self.token}
            params = {
                "user_id": self.user_id, 
                "order_id": order_id
            }
            url = urljoin(self.url_prefix, "order_status")
            r = requests.post(url, headers=headers, json=params)
            if r.status_code != 200:
                return r.status_code, ""
            return r.status_code, r.json().get("order_status")
    
    def cancel_order(self, order_id: str) -> (int, str):
        json = {
            "user_id": self.user_id,
            "order_id": order_id,
        }
        url = urljoin(self.url_prefix, "cancel_order")
        headers = {"token": self.token}
        r = requests.post(url, headers=headers, json=json)
        response_data = r.json()
        message = response_data.get("message", "No message in response")
        return r.status_code, message
    
    def get_order_history(self) -> (int, str, list):
        params = {"user_id": self.user_id}
        url = urljoin(self.url_prefix, "order_history")
        headers = {"token": self.token}
        
        try:
            r = requests.post(url, headers=headers, json=params)
            if r.status_code != 200:
                return r.status_code, r.text if r.text else "Request failed", []
            return r.status_code, "ok", r.json().get("orders", [])
        except requests.exceptions.RequestException as e:
            return 500, str(e), []
        except ValueError as e:
            return 500, "Invalid response format", []