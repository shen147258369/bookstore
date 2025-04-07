import jwt
import time
import logging
from be.model import error
from be.model.db_conn import DBConn  # 引入修改后的 DBConn

class User(DBConn):
    token_lifetime: int = 3600  # 3600 秒

    def __init__(self):
        super().__init__()  # 继承 DBConn 的初始化（包含 MongoDB 连接）
        self.user_collection = self.users  # 使用 DBConn 中定义的 users 集合

    def __check_token(self, user_id: str, db_token: str, token: str) -> bool:
        try:
            if db_token != token:
                return False
            jwt_text = jwt.decode(encoded_token=token, key=user_id, algorithms="HS256")
            ts = jwt_text["timestamp"]
            now = time.time()
            return 0 <= now - ts < self.token_lifetime  # 有效期检查
        except jwt.exceptions.InvalidSignatureError as e:
            logging.error(f"JWT 验证失败: {str(e)}")
            return False

    def register(self, user_id: str, password: str) -> tuple[int, str]:
        try:
            # 检查用户是否已存在（利用唯一索引，避免并发冲突）
            if self.user_collection.find_one({"user_id": user_id}):
                return error.error_exist_user_id(user_id)
            
            terminal = f"terminal_{time.time()}"
            token = self.jwt_encode(user_id, terminal)
            # 插入用户文档（注意 MongoDB 不需要显式 commit，自动持久化）
            self.user_collection.insert_one({
                "user_id": user_id,
                "password": password,
                "balance": 0,
                "token": token,
                "terminal": terminal
            })
        except Exception as e:
            logging.error(f"注册失败: {str(e)}")
            if "E11000" in str(e):  # 唯一索引冲突（并发场景）
                return error.error_exist_user_id(user_id)
            return 528, f"数据库错误: {str(e)}"
        return 200, "ok"

    def check_token(self, user_id: str, token: str) -> tuple[int, str]:
        user_doc = self.user_collection.find_one({"user_id": user_id})
        if not user_doc:
            return error.error_authorization_fail()
        db_token = user_doc.get("token", "")
        if not self.__check_token(user_id, db_token, token):
            return error.error_authorization_fail()
        return 200, "ok"

    def check_password(self, user_id: str, password: str) -> tuple[int, str]:
        user_doc = self.user_collection.find_one({"user_id": user_id})
        if not user_doc:
            return error.error_authorization_fail()
        if password != user_doc.get("password", ""):
            return error.error_authorization_fail()
        return 200, "ok"

    def login(self, user_id: str, password: str, terminal: str) -> tuple[int, str, str]:
        token = ""
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message, ""
            
            token = self.jwt_encode(user_id, terminal)
            # 更新 token 和 terminal（使用 $set 避免覆盖其他字段）
            update_result = self.user_collection.update_one(
                {"user_id": user_id},
                {"$set": {"token": token, "terminal": terminal}}
            )
            if update_result.modified_count == 0:
                return error.error_authorization_fail() + ("",)
        except Exception as e:
            logging.error(f"登录失败: {str(e)}")
            return 530, f"系统错误: {str(e)}", ""
        return 200, "ok", token

    def logout(self, user_id: str, token: str) -> tuple[int, str]:
        try:
            code, message = self.check_token(user_id, token)
            if code != 200:
                return code, message
            
            # 生成新 terminal 使旧 token 失效（token 依赖 terminal 生成）
            new_terminal = f"terminal_{time.time()}"
            dummy_token = self.jwt_encode(user_id, new_terminal)
            update_result = self.user_collection.update_one(
                {"user_id": user_id},
                {"$set": {"token": dummy_token, "terminal": new_terminal}}
            )
            if update_result.modified_count == 0:
                return error.error_authorization_fail()
        except Exception as e:
            logging.error(f"登出失败: {str(e)}")
            return 528, f"数据库错误: {str(e)}"
        return 200, "ok"

    def unregister(self, user_id: str, password: str) -> tuple[int, str]:
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message
            
            # 删除用户文档
            delete_result = self.user_collection.delete_one({"user_id": user_id})
            if delete_result.deleted_count == 0:
                return error.error_authorization_fail()
        except Exception as e:
            logging.error(f"注销失败: {str(e)}")
            return 528, f"数据库错误: {str(e)}"
        return 200, "ok"

    def change_password(
        self, user_id: str, old_password: str, new_password: str
    ) -> tuple[int, str]:
        try:
            code, message = self.check_password(user_id, old_password)
            if code != 200:
                return code, message
            
            new_terminal = f"terminal_{time.time()}"
            new_token = self.jwt_encode(user_id, new_terminal)
            # 更新密码、token 和 terminal
            update_result = self.user_collection.update_one(
                {"user_id": user_id},
                {"$set": {
                    "password": new_password,
                    "token": new_token,
                    "terminal": new_terminal
                }}
            )
            if update_result.modified_count == 0:
                return error.error_authorization_fail()
        except Exception as e:
            logging.error(f"修改密码失败: {str(e)}")
            return 528, f"数据库错误: {str(e)}"
        return 200, "ok"

    # 保留 JWT 工具函数（作为类方法或独立函数，此处保持原逻辑）
    @staticmethod
    def jwt_encode(user_id: str, terminal: str) -> str:
        encoded = jwt.encode(
            {"user_id": user_id, "terminal": terminal, "timestamp": time.time()},
            key=user_id,
            algorithm="HS256",
        )
        return encoded.decode("utf-8")

    @staticmethod
    def jwt_decode(encoded_token: str, user_id: str) -> dict:
        return jwt.decode(encoded_token, key=user_id, algorithms="HS256")