from flask import Blueprint
from flask import request
from flask import jsonify
from be.model.buyer import Buyer
from be.model import error 
import logging

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

    if code == 511: 
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

@bp_buyer.route("/search_books", methods=["POST"])
def search_books():
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        query = data.get("query")
        search_field = data.get("search_field", "all")
        store_id = data.get("store_id")
        page = data.get("page", 1)
        per_page = data.get("per_page", 10)

        if not user_id:
            return jsonify({"message": "User ID is required"}), 400
        if not query:
            return jsonify({"message": "Query is required"}), 400

        buyer = Buyer()
        code, message, result = buyer.search_books(
            user_id=user_id,
            query=query,
            search_field=search_field,
            store_id=store_id,
            page=page,
            per_page=per_page
        )

        if code != 200:
            return jsonify({"message": message}), code

        return jsonify({
            "message": message,
            "result": result,
            "page": result["page"],
            "per_page": result["per_page"],
            "total": result["total"],
            "total_pages": result["total_pages"]
        }), code

    except KeyError as ke:
        logging.error(f"KeyError in search_books: {str(ke)}")
        return jsonify({"message": f"Missing key in request data: {str(ke)}"}), 400
    except ValueError as ve:
        logging.error(f"ValueError in search_books: {str(ve)}")
        return jsonify({"message": f"Invalid value in request data: {str(ve)}"}), 400
    except Exception as e:
        logging.error(f"Error in search_books: {str(e)}")
        return jsonify({"message": "Internal server error"}), 500

@bp_buyer.route("/reduce_order_item", methods=["POST"])
def reduce_order_item():
    data = request.get_json()
    user_id = data.get("user_id")
    order_id = data.get("order_id")
    book_id = data.get("book_id")
    delta = data.get("delta")

    if not all([user_id, order_id, book_id, delta is not None]):
        return jsonify({"message": "Missing required parameters"}), 400

    if not isinstance(delta, int) or delta <= 0:
        return jsonify({"message": "Delta must be a positive integer"}), 400

    buyer = Buyer()
    code, message = buyer.reduce_order_item(user_id, order_id, book_id, delta)
    return jsonify({"message": message}), code