from be.model import store

class DBConn:
    def __init__(self):
        self.conn = store.get_db_conn()
        self.page_size = 5

    def user_id_exist(self, user_id):
        user_col = self.conn['user']
        result = user_col.find_one({'user_id': user_id})
        return result is not None

    def book_id_exist(self, store_id, book_id):
        book_col = self.conn['store']
        result = book_col.find_one({'store_id': store_id, 'book_id': book_id})
        return result is not None

    def store_id_exist(self, store_id):
        store_col = self.conn['user_store']
        result = store_col.find_one({'store_id': store_id})
        return result is not None