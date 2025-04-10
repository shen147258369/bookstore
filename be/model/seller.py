from be.model import error
from be.model import db_conn
import pymongo

class Seller(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def add_book(
        self,
        user_id: str,
        store_id: str,
        book_id: str,
        book_json_str: str,
        stock_level: int,
    ):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)
            if self.book_id_exist(store_id, book_id):
                return error.error_exist_book_id(book_id)

            store_col = self.conn['store']
            store_col.insert_one({
                'store_id': store_id,
                'book_id': book_id,
                'book_info': book_json_str,
                'stock_level': stock_level
            })
        except pymongo.errors.PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def add_stock_level(
        self, user_id: str, store_id: str, book_id: str, add_stock_level: int
    ):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)
            if not self.book_id_exist(store_id, book_id):
                return error.error_non_exist_book_id(book_id)

            store_col = self.conn['store']
            result = store_col.update_one(
                {'store_id': store_id, 'book_id': book_id},
                {'$inc': {'stock_level': add_stock_level}}
            )
            if result.matched_count == 0:
                return error.error_non_exist_book_id(book_id)

        except pymongo.errors.PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def create_store(self, user_id: str, store_id: str) -> (int, str):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if self.store_id_exist(store_id):
                return error.error_exist_store_id(store_id)

            user_store_col = self.conn['user_store']
            user_store_col.insert_one({
                'store_id': store_id,
                'user_id': user_id
            })
        except pymongo.errors.PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def ship_order(self, seller_id: str, store_id: str, order_id: str) -> (int, str):
        try:
            order_col = self.conn['new_order']
            order = order_col.find_one({'order_id': order_id, 'store_id': store_id})
            if not order:
                return error.error_invalid_order_id(order_id)

            status = order.get('status')

            user_store_col = self.conn['user_store']
            seller = user_store_col.find_one({'user_id': seller_id, 'store_id': store_id})
            if not seller:
                return error.error_authorization_fail()

            if status != 'paid':
                return error.error_order_status(order_id)

            result = order_col.update_one(
                {'order_id': order_id},
                {'$set': {'status': 'shipped'}}
            )
            if result.matched_count == 0:
                return error.error_invalid_order_id(order_id)

        except pymongo.errors.PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"