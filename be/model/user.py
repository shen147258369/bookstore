import jwt
import time
import logging
import mysql.connector
from be.model import error
from be.model import db_conn


def jwt_encode(user_id: str, terminal: str) -> str:
    encoded = jwt.encode(
        {"user_id": user_id, "terminal": terminal, "timestamp": time.time()},
        key=user_id,
        algorithm="HS256",
    )
    return encoded.decode("utf-8")


def jwt_decode(encoded_token, user_id: str) -> str:
    decoded = jwt.decode(encoded_token, key=user_id, algorithms="HS256")
    return decoded


class User(db_conn.DBConn):
    token_lifetime: int = 3600  # 3600 seconds

    def __init__(self):
        super().__init__()

    def __check_token(self, user_id, db_token, token) -> bool:
        try:
            if db_token != token:
                return False
            jwt_text = jwt_decode(encoded_token=token, user_id=user_id)
            ts = jwt_text.get("timestamp")
            if ts is not None:
                now = time.time()
                if 0 <= now - ts < self.token_lifetime:
                    return True
        except jwt.exceptions.InvalidSignatureError as e:
            logging.error(str(e))
        except Exception as e:
            logging.error(str(e))
        return False

    def register(self, user_id: str, password: str):
        try:
            terminal = f"terminal_{time.time()}"
            token = jwt_encode(user_id, terminal)

            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO users (user_id, password_hash, token, terminal) 
                VALUES (%s, %s, %s, %s)
            """, (user_id, password, token, terminal))
            self.conn.commit()
        except mysql.connector.Error as e:
            self.conn.rollback()
            if e.errno == 1062:  # Duplicate entry
                return error.error_exist_user_id(user_id)
            return 528, str(e)
        except BaseException as e:
            self.conn.rollback()
            return 530, str(e)
        return 200, "ok"

    def check_token(self, user_id: str, token: str) -> (int, str):
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute("SELECT token FROM users WHERE user_id = %s", (user_id,))
            user = cursor.fetchone()
            if user is None:
                return error.error_authorization_fail()
            db_token = user.get('token')
            if not self.__check_token(user_id, db_token, token):
                return error.error_authorization_fail()
            return 200, "ok"
        except mysql.connector.Error as e:
            return 528, str(e)
        except BaseException as e:
            return 530, str(e)

    def check_password(self, user_id: str, password: str) -> (int, str):
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            user = cursor.fetchone()
            if user is None:
                return error.error_authorization_fail()
            if password != user.get('password_hash'):
                return error.error_authorization_fail()
            return 200, "ok"
        except mysql.connector.Error as e:
            return 528, str(e)
        except BaseException as e:
            return 530, str(e)

    def login(self, user_id: str, password: str, terminal: str) -> (int, str, str):
        token = ""
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message, ""

            token = jwt_encode(user_id, terminal)
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE users 
                SET token = %s, terminal = %s 
                WHERE user_id = %s
            """, (token, terminal, user_id))
            if cursor.rowcount == 0:
                self.conn.rollback()
                return error.error_authorization_fail() + ("",)
            self.conn.commit()
            return 200, "ok", token
        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, str(e), ""
        except BaseException as e:
            self.conn.rollback()
            return 530, str(e), ""

    def logout(self, user_id: str, token: str) -> (int, str):
        try:
            code, message = self.check_token(user_id, token)
            if code != 200:
                return code, message

            terminal = f"terminal_{time.time()}"
            dummy_token = jwt_encode(user_id, terminal)
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE users 
                SET token = %s, terminal = %s 
                WHERE user_id = %s
            """, (dummy_token, terminal, user_id))
            if cursor.rowcount == 0:
                self.conn.rollback()
                return error.error_authorization_fail()
            self.conn.commit()
        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, str(e)
        except BaseException as e:
            self.conn.rollback()
            return 530, str(e)
        return 200, "ok"

    def unregister(self, user_id: str, password: str) -> (int, str):
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message

            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
            if cursor.rowcount == 0:
                self.conn.rollback()
                return error.error_authorization_fail()
            self.conn.commit()
        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, str(e)
        except BaseException as e:
            self.conn.rollback()
            return 530, str(e)
        return 200, "ok"

    def change_password(self, user_id: str, old_password: str, new_password: str) -> (int, str):
        try:
            code, message = self.check_password(user_id, old_password)
            if code != 200:
                return code, message

            terminal = f"terminal_{time.time()}"
            token = jwt_encode(user_id, terminal)
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE users 
                SET password_hash = %s, token = %s, terminal = %s 
                WHERE user_id = %s
            """, (new_password, token, terminal, user_id))
            if cursor.rowcount == 0:
                self.conn.rollback()
                return error.error_authorization_fail()
            self.conn.commit()
        except mysql.connector.Error as e:
            self.conn.rollback()
            return 528, str(e)
        except BaseException as e:
            self.conn.rollback()
            return 530, str(e)
        return 200, "ok"
