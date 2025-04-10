import uuid
import json
import logging
from be.model import db_conn
from be.model import error
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple

class Buyer(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def new_order(self, user_id: str, store_id: str, id_and_count: [(str, int)]) -> (int, str, str):
        order_id = ""
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)
            uid = f"{user_id}_{store_id}_{uuid.uuid1()}"
            for book_id, count in id_and_count:
                book = self.conn['store'].find_one({'store_id': store_id, 'book_id': book_id})
                if not book:
                    return error.error_non_exist_book_id(book_id) + (order_id,)

                stock = book.get('stock_level', 0)
                try:
                    book_info = json.loads(book.get('book_info', '{}'))
                    price = book_info.get('price')
                except json.JSONDecodeError:
                    logging.error(f"Failed to decode book_info for book {book_id}")
                    return 530, "Failed to decode book_info", ""

                if stock < count:
                    return error.error_stock_level_low(book_id) + (order_id,)

                result = self.conn['store'].update_one(
                    {'store_id': store_id, 'book_id': book_id, 'stock_level': {'$gte': count}},
                    {'$inc': {'stock_level': -count}}
                )
                if result.modified_count == 0:
                    return error.error_stock_level_low(book_id) + (order_id,)

                self.conn['new_order_detail'].insert_one({
                    'order_id': uid,
                    'book_id': book_id,
                    'count': count,
                    'price': price
                })

            self.conn['new_order'].insert_one({
                'order_id': uid,
                'store_id': store_id,
                'user_id': user_id,
                'status': 'unpaid',
                'order_time': datetime.now(timezone.utc)
            })
            order_id = uid
            return 200, "ok", order_id

        except Exception as e:
            logging.error("Failed to create order:", exc_info=True)
            return 530, f"Internal error: {str(e)}", ""

    def payment(self, user_id: str, password: str, order_id: str) -> (int, str):
        try:
            # 1. 验证订单基础信息
            order_col = self.conn['new_order']
            order = order_col.find_one({'order_id': order_id})
            if not order:
                return error.error_invalid_order_id(order_id)

            buyer_id, store_id, status = order.get('user_id'), order.get('store_id'), order.get('status')

            # 2. 验证权限和订单状态
            if buyer_id != user_id:
                return error.error_authorization_fail()
            if status != 'unpaid':
                return error.error_order_status(order_id)

            # 3. 验证支付密码
            user_col = self.conn['user']
            user = user_col.find_one({'user_id': buyer_id})
            if not user:
                return error.error_non_exist_user_id(buyer_id)
            balance, pwd = user.get('balance'), user.get('password')
            if password != pwd:
                return error.error_authorization_fail()

            # 4. 验证商家信息
            user_store_col = self.conn['user_store']
            seller = user_store_col.find_one({'store_id': store_id})
            if not seller:
                return error.error_non_exist_store_id(store_id)
            seller_id = seller.get('user_id')

            # 5. 计算订单总价
            total_price = 0
            order_detail_col = self.conn['new_order_detail']
            for item in order_detail_col.find({'order_id': order_id}):
                total_price += item.get('price') * item.get('count')

            # 6. 检查余额
            if balance < total_price:
                return error.error_not_sufficient_funds(order_id)

            # 7. 执行支付操作
            # 扣买家余额
            result = user_col.update_one(
                {'user_id': buyer_id, 'balance': {'$gte': total_price}},
                {'$inc': {'balance': -total_price}}
            )
            if result.matched_count == 0:
                return error.error_not_sufficient_funds(order_id)

            # 加商家余额
            user_col.update_one(
                {'user_id': seller_id},
                {'$inc': {'balance': total_price}}
            )

            # 更新订单状态为paid
            order_col.update_one(
                {'order_id': order_id},
                {'$set': {'status': 'paid'}}
            )

            return 200, "ok"

        except Exception as e:
            return 530, f"Internal error: {str(e)}"

    def add_funds(self, user_id, password, add_value) -> (int, str):
        try:
            user_col = self.conn['user']
            user = user_col.find_one({'user_id': user_id})
            if user is None:
                return error.error_authorization_fail()

            if user.get('password') != password:
                return error.error_authorization_fail()

            result = user_col.update_one(
                {'user_id': user_id},
                {'$inc': {'balance': add_value}}
            )
            if result.matched_count == 0:
                return error.error_non_exist_user_id(user_id)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return 530, "{}".format(str(e))

        return 200, "ok"

    def receive_order(self, user_id: str, order_id: str) -> (int, str):
        try:
            order_col = self.conn['new_order']
            order = order_col.find_one({'order_id': order_id})
            if order is None:
                return error.error_invalid_order_id(order_id)

            buyer_id, status = order.get('user_id'), order.get('status')
            if buyer_id != user_id:
                return error.error_authorization_fail()

            if status != "shipped":
                return error.error_order_status(order_id)
            
            result = order_col.update_one(
                {'order_id': order_id},
                {'$set': {'status': 'completed'}}
            )

            if result.matched_count == 0:
                return error.error_invalid_order_id(order_id)

        except Exception as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def get_order_status(self, user_id: str, order_id: str) -> (int, str, str):
        try:
            order_col = self.conn['new_order']
            order = order_col.find_one({'order_id': order_id})

            if not order:
                logging.warning(f"Order not found: {order_id}")
                return *error.error_invalid_order_id(order_id), ""

            if order.get('user_id') != user_id:
                logging.warning(f"Authorization failed: order_user={order.get('user_id')}, req_user={user_id}")
                return *error.error_authorization_fail(), ""

            return 200, "ok", order.get('status')

        except Exception as e:
            logging.error(f"Database error in get_order_status: {str(e)}", exc_info=True)
            return 500, f"Database error: {str(e)}", ""

    def get_order_history(self, user_id: str) -> (int, str, list):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + ([],)

            order_col = self.conn['new_order']
            orders = []
            for order in order_col.find({'user_id': user_id}).sort('order_time', -1):
                order_id, store_id, status, order_time = order.get('order_id'), order.get('store_id'), order.get('status'), order.get('order_time')

                order_detail_col = self.conn['new_order_detail']
                items = []
                total_price = 0.0
                for detail in order_detail_col.find({'order_id': order_id}):
                    book_id, count, price = detail.get('book_id'), detail.get('count'), detail.get('price')
                    items.append({
                        "book_id": book_id,
                        "count": count,
                        "price": price,
                        "subtotal": price * count
                    })
                    total_price += price * count

                orders.append({
                    "order_id": order_id,
                    "store_id": store_id,
                    "status": status,
                    "order_time": order_time,
                    "total_price": total_price,
                    "items": items
                })

            return 200, "ok", orders

        except Exception as e:
            logging.error(f"Unexpected error in get_order_history: {str(e)}")
            return 530, "{}".format(str(e))
        
    def search_books(self, user_id: str, query: str, search_field: str = 'all',
                store_id: str = None, page: int = 1, per_page: int = 10) -> Tuple[int, str, Dict]:
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (None,)
            if store_id and not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (None,)

            query_condition = {}
            if store_id:
                query_condition["store_id"] = store_id

            cursor = self.conn['store'].find(
                query_condition,
                {"_id": 0, "store_id": 1, "book_id": 1, "book_info": 1, "stock_level": 1}
            )
            books = []
            for doc in cursor:
                try:
                    book_info = json.loads(doc["book_info"]) if isinstance(doc["book_info"], str) else doc["book_info"]
                    match = False
                    query_lower = query.lower()
                    
                    if search_field == 'all':
                        for field in ['title', 'author', 'publisher', 'book_intro', 'content']:
                            field_value = str(book_info.get(field, '')).lower()
                            if query_lower in field_value:
                                match = True
                                break
                        if not match and isinstance(book_info.get('tags'), list):
                            for tag in book_info['tags']:
                                if query_lower in str(tag).lower():
                                    match = True
                                    break
                    else:
                        field_value = str(book_info.get(search_field, '')).lower()
                        if search_field == 'tags' and isinstance(field_value, list):
                            for tag in field_value:
                                if query_lower in str(tag).lower():
                                    match = True
                                    break
                        else:
                            match = query_lower in field_value

                    if match:
                        books.append({
                            "store_id": doc["store_id"],
                            "book_id": doc["book_id"],
                            "title": book_info.get("title"),
                            "author": book_info.get("author"),
                            "publisher": book_info.get("publisher"),
                            "price": book_info.get("price"),
                            "tags": book_info.get("tags", []),
                            "stock": doc.get("stock_level", 0)
                        })
                except json.JSONDecodeError:
                    logging.error(f"Failed to parse book_info for book {doc.get('book_id')}")
                    continue
                except Exception as e:
                    logging.error(f"Error processing book {doc.get('book_id')}: {str(e)}")
                    continue

            # 分页处理
            total = len(books)
            start = (page - 1) * per_page
            end = start + per_page
            paginated_books = books[start:end]

            return 200, "ok", {
                "books": paginated_books,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": max(1, (total + per_page - 1) // per_page)
            }

        except Exception as e:
            logging.error(f"Search error: {str(e)}", exc_info=True)
            return 500, f"Internal error: {str(e)}", None
        
    def cancel_order(self, user_id: str, order_id: str) -> (int, str):
        """
        取消订单
        :param user_id: 用户ID
        :param order_id: 订单ID
        :return: (状态码, 消息)
        """
        try:
            order_col = self.conn['new_order']
            order = order_col.find_one({'order_id': order_id})
            if order is None:
                return error.error_invalid_order_id(order_id)

            buyer_id, status, store_id = order.get('user_id'), order.get('status'), order.get('store_id')
            if buyer_id != user_id:
                return error.error_authorization_fail()

            if status != 'unpaid':
                return error.error_order_status(order_id)

            order_detail_col = self.conn['new_order_detail']
            for item in order_detail_col.find({'order_id': order_id}):
                book_id, count = item.get('book_id'), item.get('count')
                self.conn['store'].update_one(
                    {'store_id': store_id, 'book_id': book_id},
                    {'$inc': {'stock_level': count}}
                )

            result = order_col.update_one(
                {'order_id': order_id},
                {'$set': {'status': 'cancelled'}}
            )

            if result.matched_count == 0:
                return error.error_invalid_order_id(order_id)

        except Exception as e:
            return 530, "{}".format(str(e))

        return 200, "Order cancelled successfully"


class OrderCleaner(threading.Thread):
    def __init__(self, interval_seconds=30):
        super().__init__()
        self.interval_seconds = interval_seconds
        self.daemon = True
        self.timeout_seconds = 30
        self.running = True
        logging.info(f"OrderCleaner initialized with {self.timeout_seconds}s timeout")

    def stop(self):
        self.running = False

    def run(self):
        logging.info("OrderCleaner thread started")
        while self.running:
            try:
                db = db_conn.DBConn()
                conn = db.conn

                now = datetime.now(timezone.utc)
                threshold = now - timedelta(seconds=self.timeout_seconds)

                logging.info(f"[OrderCleaner] Checking orders before {threshold} (UTC)")

                order_col = conn['new_order']
                orders_to_cancel = list(order_col.find({
                    'status': 'unpaid',
                    'order_time': {'$lte': threshold}
                }))

                if not orders_to_cancel:
                    logging.info("[OrderCleaner] No unpaid orders to cancel")
                    time.sleep(self.interval_seconds)
                    continue

                logging.info(f"[OrderCleaner] Found {len(orders_to_cancel)} orders to cancel")

                for order in orders_to_cancel:
                    order_id, store_id = order.get('order_id'), order.get('store_id')
                    try:
                        order_detail_col = conn['new_order_detail']
                        for item in order_detail_col.find({'order_id': order_id}):
                            book_id, count = item.get('book_id'), item.get('count')
                            conn['store'].update_one(
                                {'store_id': store_id, 'book_id': book_id},
                                {'$inc': {'stock_level': count}}
                            )

                        order_col.update_one(
                            {'order_id': order_id},
                            {'$set': {'status': 'cancelled'}}
                        )

                        logging.info(f"[OrderCleaner] Successfully cancelled order {order_id}")

                    except Exception as e:
                        logging.error(f"[OrderCleaner] Failed to cancel order {order_id}: {str(e)}")

            except Exception as e:
                logging.error(f"[OrderCleaner] Critical error: {str(e)}", exc_info=True)

            finally:
                time.sleep(self.interval_seconds)

        logging.info("OrderCleaner thread stopped")