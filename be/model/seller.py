import mysql.connector
import json
from be.model import error
from be.model import db_conn


class Seller(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def check_store_owner(self, user_id: str, store_id: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM stores WHERE store_id = %s AND user_id = %s",
            (store_id, user_id)
        )
        result = cursor.fetchone()
        cursor.close()
        return result is not None

    def create_store(self, user_id: str, store_id: str) -> (int, str):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)

            if self.store_id_exist(store_id):
                return error.error_exist_store_id(store_id)

            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO stores (store_id, user_id, store_name) VALUES (%s, %s, %s)",
                (store_id, user_id, f"Store-{store_id}")
            )
            self.conn.commit()
            cursor.close()

        except mysql.connector.IntegrityError:
            self.conn.rollback()
            return error.error_exist_store_id(store_id)
        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, f"MySQL error: {str(e)}"
        except Exception as e:
            self.conn.rollback()
            return 530, f"Internal error: {str(e)}"
        return 200, "ok"

    def add_book(self, user_id: str, store_id: str, book_id: str, book_json_str: str, stock_level: int):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)

            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)

            if not self.check_store_owner(user_id, store_id):
                return error.error_authorization_fail()

            try:
                book_info = json.loads(book_json_str)
                title = book_info.get("title", "")
                author = book_info.get("author", "")
                publisher = book_info.get("publisher", "")
                price = book_info.get("price", 0)
            except json.JSONDecodeError:
                return 400, "Invalid book JSON format"

            if self.book_id_exist(store_id, book_id):
                return error.error_exist_book_id(book_id)

            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT IGNORE INTO books (id, title, author, publisher, price) "
                "VALUES (%s, %s, %s, %s, %s)",
                (book_id, title, author, publisher, price)
            )

            cursor.execute(
                "INSERT INTO store_inventory (store_id, book_id, stock_quantity, book_price) "
                "VALUES (%s, %s, %s, %s)",
                (store_id, book_id, stock_level, price)
            )

            self.conn.commit()
            cursor.close()
            return 200, "ok"

        except mysql.connector.IntegrityError as e:
            self.conn.rollback()
            return 400, f"Integrity error: {str(e)}"
        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, f"MySQL error: {str(e)}"
        except Exception as e:
            self.conn.rollback()
            return 530, f"Internal error: {str(e)}"

    def add_stock_level(self, user_id: str, store_id: str, book_id: str, add_stock_level: int):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)

            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)

            if not self.check_store_owner(user_id, store_id):
                return error.error_authorization_fail()

            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT stock_quantity FROM store_inventory "
                "WHERE store_id = %s AND book_id = %s FOR UPDATE",
                (store_id, book_id)
            )
            result = cursor.fetchone()
            if result is None:
                cursor.close()
                return error.error_non_exist_book_id(book_id)

            current_stock = result[0]
            new_stock = current_stock + add_stock_level

            if new_stock < 0:
                cursor.close()
                return 400, "Stock level cannot be negative"

            cursor.execute(
                "UPDATE store_inventory SET stock_quantity = %s "
                "WHERE store_id = %s AND book_id = %s",
                (new_stock, store_id, book_id)
            )

            self.conn.commit()
            cursor.close()
            return 200, "ok"

        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, f"MySQL error: {str(e)}"
        except Exception as e:
            self.conn.rollback()
            return 530, f"Internal error: {str(e)}"

    def ship_order(self, seller_id: str, store_id: str, order_id: str) -> (int, str):
        try:
            if not self.user_id_exist(seller_id):
                return error.error_non_exist_user_id(seller_id)

            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)

            if not self.check_store_owner(seller_id, store_id):
                return error.error_authorization_fail()

            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT order_status FROM orders WHERE order_id = %s AND store_id = %s",
                (order_id, store_id)
            )
            result = cursor.fetchone()
            if result is None:
                cursor.close()
                return error.error_invalid_order_id(order_id)

            status = result[0]
            if status != 'paid':
                cursor.close()
                return error.error_order_status(order_id)

            cursor.execute(
                "UPDATE orders SET order_status = 'shipped', ship_time = NOW() "
                "WHERE order_id = %s",
                (order_id,)
            )

            self.conn.commit()
            cursor.close()
            return 200, "ok"

        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, f"MySQL error: {str(e)}"
        except Exception as e:
            self.conn.rollback()
            return 530, f"Internal error: {str(e)}"

    def change_book_price(self, user_id: str, store_id: str, book_id: str, new_price: int) -> (int, str):
        try:
            cursor = self.conn.cursor()

            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)

            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)

            cursor.execute(
                "SELECT 1 FROM stores WHERE store_id = %s AND user_id = %s",
                (store_id, user_id)
            )
            if cursor.fetchone() is None:
                return error.error_authorization_fail()

            cursor.execute(
                "SELECT 1 FROM store_inventory WHERE store_id = %s AND book_id = %s",
                (store_id, book_id)
            )
            if cursor.fetchone() is None:
                return error.error_non_exist_book_id(book_id)

            cursor.execute(
                "UPDATE store_inventory SET book_price = %s WHERE store_id = %s AND book_id = %s",
                (new_price, store_id, book_id)
            )

            self.conn.commit()
            cursor.close()
            return 200, "ok"

        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, f"MySQL error: {str(e)}"
        except Exception as e:
            self.conn.rollback()
            return 530, f"Internal error: {str(e)}"
