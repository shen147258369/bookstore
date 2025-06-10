from be.model import store

class DBConn:
    def __init__(self):
        self.conn = store.get_db_conn()
        self.page_size = 5

    def user_id_exist(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
        result = cursor.fetchone()
        cursor.close()
        return result is not None

    def book_id_exist(self, store_id, book_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM store_inventory WHERE store_id = %s AND book_id = %s", (store_id, book_id))
        result = cursor.fetchone()
        cursor.close()
        return result is not None

    def store_id_exist(self, store_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM stores WHERE store_id = %s", (store_id,))
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    
    def check_store_owner(self, store_id, user_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM stores WHERE store_id = %s AND user_id = %s", (store_id, user_id))
        return cursor.fetchone() is not None
