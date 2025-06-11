import uuid
import json
import logging
from be.model import db_conn
from be.model import error
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
from decimal import Decimal
from be.model.store import get_db_conn

class Buyer(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def new_order(self, user_id: str, store_id: str, id_and_count: List[Tuple[str, int]]) -> Tuple[int, str, str]:
        order_id = ""
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)

            with self.conn.cursor() as cursor:
                # Step 1: 查找是否有该用户在该店铺下的未支付订单
                cursor.execute(
                    "SELECT order_id FROM orders "
                    "WHERE user_id = %s AND store_id = %s AND order_status = 'unpaid' FOR UPDATE",
                    (user_id, store_id)
                )
                existing_order = cursor.fetchone()

                if existing_order:
                    order_id = existing_order[0]
                else:
                    # 创建新订单
                    order_id = f"{user_id}_{store_id}_{uuid.uuid1()}"
                    cursor.execute(
                        "INSERT INTO orders (order_id, user_id, store_id, order_status, total_amount) "
                        "VALUES (%s, %s, %s, 'unpaid', 0)",
                        (order_id, user_id, store_id)
                    )

                total_price = Decimal('0.00')

                for book_id, count in id_and_count:
                    cursor.execute(
                        "SELECT stock_quantity, book_price FROM store_inventory "
                        "WHERE store_id = %s AND book_id = %s FOR UPDATE",
                        (store_id, book_id))
                    book = cursor.fetchone()
                    if not book:
                        self.conn.rollback()
                        return error.error_non_exist_book_id(book_id) + (order_id,)

                    stock, price = book
                    if stock < count:
                        self.conn.rollback()
                        return error.error_stock_level_low(book_id) + (order_id,)

                    # 检查该订单中是否已有该书籍
                    cursor.execute(
                        "SELECT quantity FROM order_details "
                        "WHERE order_id = %s AND book_id = %s",
                        (order_id, book_id)
                    )
                    existing_detail = cursor.fetchone()

                    if existing_detail:
                        new_count = existing_detail[0] + count
                        # 更新已有明细
                        cursor.execute(
                            "UPDATE order_details SET quantity = %s WHERE order_id = %s AND book_id = %s",
                            (new_count, order_id, book_id)
                        )
                    else:
                        # 插入新明细
                        cursor.execute(
                            "INSERT INTO order_details (order_id, book_id, quantity, unit_price) "
                            "VALUES (%s, %s, %s, %s)",
                            (order_id, book_id, count, price)
                        )

                    total_price += price * Decimal(count)

                # 更新订单总价
                cursor.execute(
                    "UPDATE orders SET total_amount = total_amount + %s WHERE order_id = %s",
                    (total_price, order_id)
                )

                self.conn.commit()
                return 200, "ok", order_id

        except Exception as e:
            self.conn.rollback()
            logging.error(f"Failed to create or update order: {str(e)}", exc_info=True)
            return 530, f"Internal error: {str(e)}", ""
    

    def reduce_order_item(self, user_id: str, order_id: str, book_id: str, delta: int) -> Tuple[int, str]:
        try:
            with self.conn.cursor() as cursor:
                # Step 1: 获取订单信息并加锁
                cursor.execute(
                    "SELECT o.user_id, o.store_id, o.order_status, d.quantity, d.unit_price "
                    "FROM orders o "
                    "JOIN order_details d ON o.order_id = d.order_id "
                    "WHERE o.order_id = %s AND d.book_id = %s FOR UPDATE",
                    (order_id, book_id)
                )
                result = cursor.fetchone()
                if not result:
                    return error.error_invalid_order_id(order_id)

                buyer_id, store_id, status, count, price = result

                # Step 2: 校验用户权限和订单状态
                if buyer_id != user_id:
                    return error.error_authorization_fail()
                if status != 'unpaid':
                    return error.error_order_status(order_id)

                # Step 3: 计算新数量
                new_count = count - delta
                if new_count < 0:
                    return 520, "Reduced quantity exceeds current amount"

                if new_count == 0:
                    # Step 4: 删除该条目并返还全部库存
                    cursor.execute(
                        "UPDATE store_inventory SET stock_quantity = stock_quantity + %s "
                        "WHERE store_id = %s AND book_id = %s",
                        (count, store_id, book_id)
                    )

                    cursor.execute(
                        "DELETE FROM order_details WHERE order_id = %s AND book_id = %s",
                        (order_id, book_id)
                    )

                else:
                    # Step 5: 更新数量并返还部分库存
                    cursor.execute(
                        "UPDATE store_inventory SET stock_quantity = stock_quantity + %s "
                        "WHERE store_id = %s AND book_id = %s",
                        (delta, store_id, book_id)
                    )

                    cursor.execute(
                        "UPDATE order_details SET quantity = %s "
                        "WHERE order_id = %s AND book_id = %s",
                        (new_count, order_id, book_id)
                    )

                # Step 6: 更新订单总价（重新计算）
                cursor.execute(
                    "UPDATE orders o SET total_amount = ("
                    "   SELECT COALESCE(SUM(quantity * unit_price), 0) "
                    "   FROM order_details d WHERE d.order_id = o.order_id"
                    ") WHERE o.order_id = %s",
                    (order_id,)
                )

                self.conn.commit()
                return 200, "ok"

        except Exception as e:
            self.conn.rollback()
            logging.error(f"Failed to reduce order item: {str(e)}")
            return 530, f"Internal error: {str(e)}"
        
    def payment(self, user_id: str, password: str, order_id: str) -> Tuple[int, str]:
        try:
            with self.conn.cursor() as cursor:
                # 获取订单信息并加锁
                cursor.execute(
                    "SELECT user_id, store_id, order_status FROM orders WHERE order_id = %s FOR UPDATE",
                    (order_id,)
                )
                order = cursor.fetchone()
                if not order:
                    return error.error_invalid_order_id(order_id)

                buyer_id, store_id, status = order

                if buyer_id != user_id:
                    return error.error_authorization_fail()
                if status != 'unpaid':
                    return error.error_order_status(order_id)

                # 新增：从 order_details 重新计算总价
                cursor.execute(
                    "SELECT SUM(quantity * unit_price) FROM order_details WHERE order_id = %s",
                    (order_id,)
                )
                calculated_total = cursor.fetchone()[0]

                if calculated_total is None:
                    calculated_total = Decimal('0.00')

                # 获取买家账户信息
                cursor.execute(
                    "SELECT balance, password_hash FROM users WHERE user_id = %s FOR UPDATE",
                    (buyer_id,)
                )
                user = cursor.fetchone()
                if not user:
                    return error.error_non_exist_user_id(buyer_id)

                balance, pwd = user
                if password != pwd:
                    return error.error_authorization_fail()

                # 获取卖家账户
                cursor.execute(
                    "SELECT user_id FROM stores WHERE store_id = %s",
                    (store_id,)
                )
                seller = cursor.fetchone()
                if not seller:
                    return error.error_non_exist_store_id(store_id)
                seller_id = seller[0]

                # 判断余额是否足够
                if balance < calculated_total:
                    return error.error_not_sufficient_funds(order_id)

                # 扣款和付款
                cursor.execute(
                    "UPDATE users SET balance = balance - %s WHERE user_id = %s AND balance >= %s",
                    (calculated_total, buyer_id, calculated_total)
                )
                if cursor.rowcount == 0:
                    return error.error_not_sufficient_funds(order_id)

                cursor.execute(
                    "UPDATE users SET balance = balance + %s WHERE user_id = %s",
                    (calculated_total, seller_id)
                )

                # 更新订单状态为已支付
                cursor.execute(
                    "UPDATE orders SET order_status = 'paid', total_amount = %s WHERE order_id = %s",
                    (calculated_total, order_id)
                )

                # 减少库存
                cursor.execute(
                    "UPDATE store_inventory s "
                    "JOIN order_details d ON s.book_id = d.book_id AND s.store_id = %s "
                    "SET s.stock_quantity = s.stock_quantity - d.quantity "
                    "WHERE d.order_id = %s",
                    (store_id, order_id)
                )

                self.conn.commit()
                return 200, "ok"

        except Exception as e:
            self.conn.rollback()
            logging.error(f"Payment failed: {str(e)}")
            return 530, f"Internal error: {str(e)}"

    def cancel_order(self, user_id: str, order_id: str) -> Tuple[int, str]:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT user_id, store_id, order_status "
                    "FROM orders WHERE order_id = %s FOR UPDATE",
                    (order_id,)
                )
                order = cursor.fetchone()
                if not order:
                    return error.error_invalid_order_id(order_id)
                
                buyer_id, store_id, status = order
                if buyer_id != user_id:
                    return error.error_authorization_fail()
                
                if status != 'unpaid':
                    return error.error_order_status(order_id)
                
                cursor.execute(
                    "SELECT book_id, quantity FROM order_details WHERE order_id = %s",
                    (order_id,)
                )
                details = cursor.fetchall()
                
                for book_id, count in details:
                    cursor.execute(
                        "UPDATE store_inventory SET stock_quantity = stock_quantity + %s "
                        "WHERE store_id = %s AND book_id = %s",
                        (count, store_id, book_id)
                    )
                
                cursor.execute(
                    "UPDATE orders SET order_status = 'cancelled' "
                    "WHERE order_id = %s",
                    (order_id,)
                )
                
                self.conn.commit()
                return 200, "Order cancelled successfully"
                
        except Exception as e:
            self.conn.rollback()
            return 530, f"Internal error: {str(e)}"

    def add_funds(self, user_id: str, password: str, add_value: float) -> Tuple[int, str]:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT password_hash FROM users WHERE user_id = %s",
                    (user_id,)
                )
                user = cursor.fetchone()
                if not user:
                    return error.error_authorization_fail()
                
                if user[0] != password:
                    return error.error_authorization_fail()
                
                cursor.execute(
                    "UPDATE users SET balance = balance + %s "
                    "WHERE user_id = %s",
                    (add_value, user_id)
                )
                
                if cursor.rowcount == 0:
                    return error.error_non_exist_user_id(user_id)
                
                self.conn.commit()
                return 200, "ok"
                
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error in add_funds: {str(e)}", exc_info=True)
            return 530, f"Internal error: {str(e)}"
        
    def receive_order(self, user_id: str, order_id: str) -> Tuple[int, str]:
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT user_id, order_status FROM orders WHERE order_id = %s FOR UPDATE",
                        (order_id,)
                    )
                    order = cursor.fetchone()
                    if not order:
                        return error.error_invalid_order_id(order_id)
                    
                    buyer_id, status = order
                    if buyer_id != user_id:
                        return error.error_authorization_fail()
                    
                    if status != "shipped":
                        return error.error_order_status(order_id)
                    
                    cursor.execute(
                        "UPDATE orders SET order_status = 'completed', receive_time = CURRENT_TIMESTAMP "
                        "WHERE order_id = %s",
                        (order_id,)
                    )
                    
                    self.conn.commit()
                    return 200, "ok"
                    
            except Exception as e:
                self.conn.rollback()
                return 530, f"Internal error: {str(e)}"

    def get_order_status(self, user_id: str, order_id: str) -> Tuple[int, str, str]:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT user_id, order_status FROM orders WHERE order_id = %s",
                    (order_id,)
                )
                order = cursor.fetchone()
                
                if not order:
                    logging.warning(f"Order not found: {order_id}")
                    return *error.error_invalid_order_id(order_id), ""
                
                if order[0] != user_id:
                    logging.warning(f"Authorization failed: order_user={order[0]}, req_user={user_id}")
                    return *error.error_authorization_fail(), ""
                
                return 200, "ok", order[1]
                
        except Exception as e:
            logging.error(f"Database error in get_order_status: {str(e)}", exc_info=True)
            return 500, f"Database error: {str(e)}", ""



    def get_order_history(self, user_id: str) -> Tuple[int, str, list]:
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + ([],)
                
            with self.conn.cursor(dictionary=True) as cursor:

                cursor.execute(
                    "SELECT order_id, store_id, order_status, create_time, total_amount "
                    "FROM orders WHERE user_id = %s ORDER BY create_time DESC",
                    (user_id,)
                )
                orders = []
                
                for order in cursor.fetchall():
                    order_id = order['order_id']
                    
                    cursor.execute(
                        "SELECT book_id, quantity, unit_price "
                        "FROM order_details WHERE order_id = %s",
                        (order_id,)
                    )
                    items = []
                    
                    for detail in cursor.fetchall():
                        unit_price = float(detail['unit_price']) if isinstance(detail['unit_price'], Decimal) else detail['unit_price']
                        quantity = detail['quantity']
                        items.append({
                            "book_id": detail['book_id'],
                            "count": quantity,
                            "price": unit_price,
                            "subtotal": float(unit_price * quantity)
                        })
                    
                    total_price = float(order['total_amount']) if isinstance(order['total_amount'], Decimal) else order['total_amount']
                    
                    orders.append({
                        "order_id": order_id,
                        "store_id": order['store_id'],
                        "status": order['order_status'],
                        "order_time": order['create_time'],
                        "total_price": total_price,
                        "items": items
                    })
                
                return 200, "ok", orders
                
        except Exception as e:
            logging.error(f"Unexpected error in get_order_history: {str(e)}")
            return 530, f"Internal error: {str(e)}", []


    def search_books(self, user_id: str, query: str, search_field: str = 'all',
                    store_id: str = None, page: int = 1, per_page: int = 10) -> Tuple[int, str, Dict]:
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (None,)
            if store_id and not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (None,)

            offset = (page - 1) * per_page

            with self.conn.cursor(dictionary=True) as cursor:
                base_query = """
                    SELECT si.store_id, si.book_id, b.title, b.author, b.publisher, 
                        si.book_price AS price, si.stock_quantity AS stock,
                        GROUP_CONCAT(t.name) AS tags
                    FROM store_inventory si
                    JOIN books b ON si.book_id = b.id
                    LEFT JOIN book_tags bt ON bt.book_id = b.id
                    LEFT JOIN tags t ON bt.tag_id = t.id
                """

                where_clauses = []
                params = []

                if store_id:
                    where_clauses.append("si.store_id = %s")
                    params.append(store_id)

                if search_field == 'all':
                    where_clauses.append("""
                        (
                            MATCH(b.title, b.author, b.publisher, b.book_intro, b.content) AGAINST (%s IN NATURAL LANGUAGE MODE)
                            OR b.title LIKE %s
                            OR EXISTS (
                                SELECT 1 FROM book_tags bt2 
                                JOIN tags t2 ON bt2.tag_id = t2.id 
                                WHERE bt2.book_id = b.id AND t2.name = %s
                            )
                        )
                    """)
                    params.extend([query, f"%{query}%", query])

                elif search_field == 'tags':
                    where_clauses.append("""
                        EXISTS (
                            SELECT 1 FROM book_tags bt2 
                            JOIN tags t2 ON bt2.tag_id = t2.id 
                            WHERE bt2.book_id = b.id AND t2.name = %s
                        )
                    """)
                    params.append(query)
                else:
                    allowed_fields = {'title', 'author', 'publisher', 'book_intro', 'content'}
                    if search_field not in allowed_fields:
                        return 400, f"Invalid search_field: {search_field}", None

                    if search_field == 'publisher':
                        where_clauses.append("b.publisher = %s")
                        params.append(query)
                    elif search_field == 'title':
                        where_clauses.append("b.title LIKE %s")
                        params.append(f"%{query}%")
                    else:
                        where_clauses.append(f"MATCH(b.{search_field}) AGAINST (%s IN NATURAL LANGUAGE MODE)")
                        params.append(query)


                if where_clauses:
                    base_query += " WHERE " + " AND ".join(where_clauses)

                base_query += """
                    GROUP BY si.store_id, si.book_id, b.title, b.author, b.publisher, si.book_price, si.stock_quantity
                """

                # 查询总数
                count_query = f"SELECT COUNT(*) AS total FROM ({base_query}) AS subquery"
                cursor.execute(count_query, params)
                total = cursor.fetchone()['total']

                # 添加分页限制
                base_query += " LIMIT %s OFFSET %s"
                params.extend([per_page, offset])
                cursor.execute(base_query, params)

                books = []
                for row in cursor.fetchall():
                    tags_str = row.get('tags')
                    tags_list = tags_str.split(',') if tags_str else []

                    books.append({
                        "store_id": row['store_id'],
                        "book_id": row['book_id'],
                        "title": row['title'],
                        "author": row['author'],
                        "publisher": row['publisher'],
                        "price": float(row['price']),
                        "tags": tags_list,
                        "stock": row['stock']
                    })

                return 200, "ok", {
                    "books": books,
                    "total": total,
                    "page": page,
                    "per_page": per_page,
                    "total_pages": max(1, (total + per_page - 1) // per_page)
                }

        except Exception as e:
            logging.error(f"Search error: {str(e)}", exc_info=True)
            return 500, f"Internal error: {str(e)}", None
 
class OrderCleaner(threading.Thread):
    def __init__(self, interval_seconds=30):
        super().__init__()
        self.interval_seconds = interval_seconds
        self.daemon = True
        self.timeout_seconds = 10  # 订单超时时间（秒）
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
                cursor = conn.cursor()

                # 计算超时时间点（UTC时间）
                timeout_threshold = datetime.now(timezone.utc) - timedelta(seconds=self.timeout_seconds)
                
                # 将时间阈值转换为字符串以便记录
                logging.info(f"[OrderCleaner] Checking orders before {timeout_threshold} (UTC)")

                # 查询需要取消的超时订单
                cursor.execute("""
                    SELECT order_id, store_id, create_time 
                    FROM orders 
                    WHERE order_status = 'unpaid' 
                    AND create_time <= %s
                """, (timeout_threshold,))
                orders_to_cancel = cursor.fetchall()

                if not orders_to_cancel:
                    logging.info("[OrderCleaner] No unpaid orders to cancel")
                    time.sleep(self.interval_seconds)
                    continue

                logging.info(f"[OrderCleaner] Found {len(orders_to_cancel)} orders to cancel")

                for order in orders_to_cancel:
                    order_id, store_id, create_time = order
                    try:
                        # 记录一下这个订单的创建时间和当前时间，用于调试
                        now_utc = datetime.now(timezone.utc)
                        logging.info(f"[OrderCleaner] Processing order {order_id}, created at {create_time} (UTC), now is {now_utc} (UTC)")
                        
                        # 恢复库存
                        cursor.execute("""
                            SELECT od.book_id, od.quantity
                            FROM order_details od
                            WHERE od.order_id = %s
                        """, (order_id,))
                        items = cursor.fetchall()
                        
                        for book_id, quantity in items:
                            cursor.execute("""
                                UPDATE store_inventory 
                                SET stock_quantity = stock_quantity + %s 
                                WHERE store_id = %s AND book_id = %s
                            """, (quantity, store_id, book_id))
                        
                        # 更新订单状态
                        cursor.execute("""
                            UPDATE orders 
                            SET order_status = 'cancelled' 
                            WHERE order_id = %s
                        """, (order_id,))
                        
                        conn.commit()
                        logging.info(f"[OrderCleaner] Successfully cancelled order {order_id}")

                    except Exception as e:
                        conn.rollback()
                        logging.error(f"[OrderCleaner] Failed to cancel order {order_id}: {str(e)}", exc_info=True)

            except Exception as e:
                logging.error(f"[OrderCleaner] Critical error: {str(e)}", exc_info=True)
            finally:
                time.sleep(self.interval_seconds)

        logging.info("OrderCleaner thread stopped")

AUTO_CANCEL_SECONDS = 20

def auto_cancel_unpaid_orders():
    def cancel_loop():
        while True:
            try:
                conn = get_db_conn()
                cursor = conn.cursor()

                time_threshold = datetime.now() - timedelta(seconds=AUTO_CANCEL_SECONDS)
                time_str = time_threshold.strftime("%Y-%m-%d %H:%M:%S")
                print(f"[DEBUG] Cancelling orders before {time_str}")

                sql = """
                    UPDATE orders
                    SET order_status = 'cancelled'
                    WHERE order_status = 'unpaid'
                      AND create_time < %s
                """
                cursor.execute(sql, (time_str,))
                conn.commit()
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"[auto_cancel_unpaid_orders] Error: {e}")

            time.sleep(5)

    thread = threading.Thread(target=cancel_loop, daemon=True)
    thread.start()
