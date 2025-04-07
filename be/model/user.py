import jwt
import time
import logging
from typing import Tuple
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from be.model import error
from be.model.db_conn import DBConn

# encode a json string like:
#   {
#       "user_id": [user name],
#       "terminal": [terminal code],
#       "timestamp": [ts]} to a JWT
#   }
def jwt_encode(user_id: str, terminal: str) -> str:
    encoded = jwt.encode(
        {"user_id": user_id, "terminal": terminal, "timestamp": time.time()},
        key=user_id,
        algorithm="HS256"
    )
    return encoded.decode("utf-8")

# decode a JWT to a json string like:
#   {
#       "user_id": [user name],
#       "terminal": [terminal code],
#       "timestamp": [ts]} to a JWT
#   }
def jwt_decode(encoded_token, user_id: str) -> dict:
    try:
        decoded = jwt.decode(encoded_token, key=user_id, algorithms="HS256")
        return decoded
    except jwt.exceptions.InvalidSignatureError as e:
        logging.error(str(e))
        return {}


class User(DBConn):
    token_lifetime: int = 3600  # 3600 second

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
                if self.token_lifetime > now - ts >= 0:
                    return True
        except jwt.exceptions.InvalidSignatureError as e:
            logging.error(str(e))
            return False
        return False

    def register(self, user_id: str, password: str) -> Tuple[int, str]:
        try:
            terminal = f"terminal_{str(time.time())}"
            token = jwt_encode(user_id, terminal)
            self.users.insert_one({
                "user_id": user_id,
                "password": password,
                "balance": 0,
                "token": token,
                "terminal": terminal
            })
        except PyMongoError as e:
            if "E11000" in str(e):  # 假设 MongoDB 唯一索引冲突错误包含 "E11000"
                return error.error_exist_user_id(user_id)
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)
        return 200, "ok"

    def check_token(self, user_id: str, token: str) -> Tuple[int, str]:
        user = self.users.find_one({"user_id": user_id})
        if not user:
            return error.error_authorization_fail()
        db_token = user.get("token")
        if not self.__check_token(user_id, db_token, token):
            return error.error_authorization_fail()
        return 200, "ok"

    def check_password(self, user_id: str, password: str) -> Tuple[int, str]:
        user = self.users.find_one({"user_id": user_id})
        if not user:
            return error.error_authorization_fail()
        if password != user.get("password"):
            return error.error_authorization_fail()
        return 200, "ok"

    def login(self, user_id: str, password: str, terminal: str) -> Tuple[int, str, str]:
        token = ""
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message, ""

            token = jwt_encode(user_id, terminal)
            result = self.users.update_one(
                {"user_id": user_id},
                {"$set": {"token": token, "terminal": terminal}}
            )
            if result.modified_count == 0:
                return error.error_authorization_fail() + ("",)
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e), ""
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e), ""
        return 200, "ok", token

    def logout(self, user_id: str, token: str) -> Tuple[int, str]:
        try:
            code, message = self.check_token(user_id, token)
            if code != 200:
                return code, message

            terminal = f"terminal_{str(time.time())}"
            dummy_token = jwt_encode(user_id, terminal)
            result = self.users.update_one(
                {"user_id": user_id},
                {"$set": {"token": dummy_token, "terminal": terminal}}
            )
            if result.modified_count == 0:
                return error.error_authorization_fail()
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)
        return 200, "ok"

    def unregister(self, user_id: str, password: str) -> Tuple[int, str]:
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message

            result = self.users.delete_one({"user_id": user_id})
            if result.deleted_count == 0:
                return error.error_authorization_fail()
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)
        return 200, "ok"

    def change_password(self, user_id: str, old_password: str, new_password: str) -> Tuple[int, str]:
        try:
            code, message = self.check_password(user_id, old_password)
            if code != 200:
                return code, message

            terminal = f"terminal_{str(time.time())}"
            token = jwt_encode(user_id, terminal)
            result = self.users.update_one(
                {"user_id": user_id},
                {"$set": {"password": new_password, "token": token, "terminal": terminal}}
            )
            if result.modified_count == 0:
                return error.error_authorization_fail()
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}")
            return 528, str(e)
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return 530, str(e)
        return 200, "ok"