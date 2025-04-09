from flask import Blueprint
from flask import request
from flask import jsonify
from be.model.buyer import Buyer
from be.model import error 

bp_buyer = Blueprint("buyer", __name__, url_prefix="/buyer")


@bp_buyer.route("/new_order", methods=["POST"])
def new_order():
    data = request.get_json()
    user_id = data.get("user_id")
    store_id = data.get("store_id")
    books = data.get("books", [])
    id_and_count = [(book["id"], book["count"]) for book in books]

    buyer = Buyer()
    code, message, order_id = buyer.new_order(user_id, store_id, id_and_count)
    return jsonify({"message": message, "order_id": order_id}), code

@bp_buyer.route("/payment", methods=["POST"])
def payment():
    user_id: str = request.json.get("user_id")
    order_id: str = request.json.get("order_id")
    password: str = request.json.get("password")
    b = Buyer()
    code, message = b.payment(user_id, password, order_id)
    return jsonify({"message": message}), code


@bp_buyer.route("/add_funds", methods=["POST"])
def add_funds():
    user_id = request.json.get("user_id")
    password = request.json.get("password")
    add_value = request.json.get("add_value")
    b = Buyer()
    code, message = b.add_funds(user_id, password, add_value)
    return jsonify({"message": message}), code

@bp_buyer.route("/order_status", methods=["POST"])
def get_order_status():
    user_id: str = request.json.get("user_id")
    order_id: str = request.json.get("order_id")
    if not user_id or not order_id:
        return jsonify({
            "status": 400,
            "message": "Missing user_id or order_id",
            "order_status": None
        }), 400

    b = Buyer()
    code, message, status = b.get_order_status(user_id, order_id)
    return jsonify({
        "status": code,
        "message": message,
        "order_status": status if code == 200 else None
    }), code

@bp_buyer.route("/receive_order", methods=["POST"])
def receive_order():
    user_id = request.json.get("user_id")
    order_id = request.json.get("order_id")
    if not user_id or not order_id:
        return jsonify({"message": "Missing user_id or order_id"}), 400

    b = Buyer()
    code, message = b.receive_order(user_id, order_id)

    if code != 200:
        return jsonify({"message": message}), code

    return jsonify({"message": "Order received successfully"}), 200

@bp_buyer.route("/cancel_order", methods=["POST"])
def cancel_order():
    user_id = request.json.get("user_id")
    order_id = request.json.get("order_id")
    if not user_id or not order_id:
        return jsonify({"message": "Missing user_id or order_id"}), 400

    b = Buyer()
    code, message = b.cancel_order(user_id, order_id)
    
    if code != 200:
        return jsonify({"message": message}), code

    return jsonify({"message": "Order cancelled successfully"}), 200


@bp_buyer.route("/order_history", methods=["POST"])
def get_order_history():
    user_id: str = request.json.get("user_id")

    if not user_id:
        return jsonify({"message": "Missing user_id"}), 400

    b = Buyer()
    code, message, orders = b.get_order_history(user_id)

    if code == 511:  # non exist user id
        return jsonify({"message": f"User not found: {user_id}"}), 400
    elif code != 200:
        return jsonify({"message": message}), code

    formatted_orders: list[dict] = []
    for order in orders:
        formatted_order = {
            **order,
            "order_time": order["order_time"] if order.get("order_time") else None
        }
        formatted_orders.append(formatted_order)

    return jsonify({"orders": formatted_orders}), 200
