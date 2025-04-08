import uuid
import json
import logging
from be.model import db_conn
from be.model import error
import threading
import time
from datetime import datetime, timedelta, timezone


class Buyer(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def new_order(
        self, user_id: str, store_id: str, id_and_count: [(str, int)]
    ) -> (int, str, str):
        order_id = ""
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)
            uid = "{}_{}_{}".format(user_id, store_id, str(uuid.uuid1()))

            for book_id, count in id_and_count:
                book_col = self.conn['store']
                book = book_col.find_one({'store_id': store_id, 'book_id': book_id})
                if book is None:
                    return error.error_non_exist_book_id(book_id) + (order_id,)

                stock_level = book.get('stock_level', 0)
                book_info = book.get('book_info', {})
                price = book_info.get("price")

                if stock_level < count:
                    return error.error_stock_level_low(book_id) + (order_id,)

                result = book_col.update_one(
                    {'store_id': store_id, 'book_id': book_id, 'stock_level': {'$gte': count}},
                    {'$inc': {'stock_level': -count}}
                )
                if result.matched_count == 0:
                    return error.error_stock_level_low(book_id) + (order_id,)

                order_detail_col = self.conn['new_order_detail']
                order_detail_col.insert_one({
                    'order_id': uid,
                    'book_id': book_id,
                    'count': count,
                    'price': price
                })

            order_col = self.conn['new_order']
            order_col.insert_one({
                'order_id': uid,
                'store_id': store_id,
                'user_id': user_id,
                'status': 'unpaid',
                'order_time': datetime.now(timezone.utc)
            })
            order_id = uid
        except Exception as e:
            logging.info("530, {}".format(str(e)))
            return 530, "{}".format(str(e)), ""

        return 200, "ok", order_id

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
                return error.error_order_status(order_id)  # 确保是未支付订单

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

            # 7. 执行支付事务
            with self.conn.client.start_session() as session:
                with session.start_transaction():
                    # 扣买家余额
                    user_col.update_one(
                        {'user_id': buyer_id, 'balance': {'$gte': total_price}},
                        {'$inc': {'balance': -total_price}},
                        session=session
                    )

                    # 加商家余额
                    user_col.update_one(
                        {'user_id': seller_id},
                        {'$inc': {'balance': total_price}},
                        session=session
                    )

                    # 更新订单状态为paid
                    order_col.update_one(
                        {'order_id': order_id},
                        {'$set': {'status': 'paid'}},
                        session=session
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
            return 530, "{}".format(str(e))

        return 200, "ok"

    def receive_order(self, user_id: str, order_id: str) -> (int, str):
        """
        买家确认收货
        :param user_id: 买家ID
        :param order_id: 订单ID
        :return: (状态码, 消息)
        """
        try:
            # 检查订单是否存在且属于该买家
            order_col = self.conn['new_order']
            order = order_col.find_one({'order_id': order_id})
            if order is None:
                return error.error_invalid_order_id(order_id)

            buyer_id, status = order.get('user_id'), order.get('status')

            # 验证订单是否属于该买家
            if buyer_id != user_id:
                return error.error_authorization_fail()

            # 检查订单状态是否为已发货
            if status != "shipped":
                return error.error_order_status(order_id)

            # 更新订单状态为已完成
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
        """
        获取用户的历史订单
        :param user_id: 用户ID
        :return: (状态码, 消息, 订单列表)
        """
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + ([],)

            # 查询所有订单基本信息
            order_col = self.conn['new_order']
            orders = []
            for order in order_col.find({'user_id': user_id}).sort('order_time', -1):
                order_id, store_id, status, order_time = order.get('order_id'), order.get('store_id'), order.get('status'), order.get('order_time')

                # 查询订单详情
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
            return 530, "{}".format(str(e)), []

    def cancel_order(self, user_id: str, order_id: str) -> (int, str):
        """
        取消订单
        :param user_id: 用户ID
        :param order_id: 订单ID
        :return: (状态码, 消息)
        """
        try:
            # 检查订单是否存在且属于该用户
            order_col = self.conn['new_order']
            order = order_col.find_one({'order_id': order_id})
            if order is None:
                return error.error_invalid_order_id(order_id)

            buyer_id, status, store_id = order.get('user_id'), order.get('status'), order.get('store_id')

            # 验证订单是否属于该用户
            if buyer_id != user_id:
                return error.error_authorization_fail()

            # 检查订单状态是否为未支付
            if status != 'unpaid':
                return error.error_order_status(order_id)

            # 恢复库存
            order_detail_col = self.conn['new_order_detail']
            for item in order_detail_col.find({'order_id': order_id}):
                book_id, count = item.get('book_id'), item.get('count')
                self.conn['store'].update_one(
                    {'store_id': store_id, 'book_id': book_id},
                    {'$inc': {'stock_level': count}}
                )

            # 更新订单状态
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
                        with conn.client.start_session() as session:
                            with session.start_transaction():
                                order_detail_col = conn['new_order_detail']
                                for item in order_detail_col.find({'order_id': order_id}):
                                    book_id, count = item.get('book_id'), item.get('count')
                                    conn['store'].update_one(
                                        {'store_id': store_id, 'book_id': book_id},
                                        {'$inc': {'stock_level': count}},
                                        session=session
                                    )

                                order_col.update_one(
                                    {'order_id': order_id},
                                    {'$set': {'status': 'cancelled'}},
                                    session=session
                                )

                        logging.info(f"[OrderCleaner] Successfully cancelled order {order_id}")

                    except Exception as e:
                        logging.error(f"[OrderCleaner] Failed to cancel order {order_id}: {str(e)}")

            except Exception as e:
                logging.error(f"[OrderCleaner] Critical error: {str(e)}", exc_info=True)

            finally:
                time.sleep(self.interval_seconds)

        logging.info("OrderCleaner thread stopped")