from flask import Blueprint
from flask import request
from flask import jsonify
from be.model import seller
import json

bp_seller = Blueprint("seller", __name__, url_prefix="/seller")


@bp_seller.route("/create_store", methods=["POST"])
def seller_create_store():
    user_id: str = request.json.get("user_id")
    store_id: str = request.json.get("store_id")
    s = seller.Seller()
    code, message = s.create_store(user_id, store_id)
    return jsonify({"message": message}), code

@bp_seller.route("/change_store_name", methods=["POST"])
def change_store_name():
    user_id: str = request.json.get("user_id")
    store_id: str = request.json.get("store_id")
    new_name: str = request.json.get("new_name")

    if not new_name:
        return jsonify({"message": "店铺名称不能为空"}), 400

    s = seller.Seller()
    code, message = s.change_store_name(user_id, store_id, new_name)
    
    return jsonify({"message": message}), code

@bp_seller.route("/add_book", methods=["POST"])
def seller_add_book():
    user_id: str = request.json.get("user_id")
    store_id: str = request.json.get("store_id")
    book_info: str = request.json.get("book_info")
    stock_level: str = request.json.get("stock_level", 0)

    s = seller.Seller()
    code, message = s.add_book(
        user_id, store_id, book_info.get("id"), json.dumps(book_info), stock_level
    )

    return jsonify({"message": message}), code


@bp_seller.route("/add_stock_level", methods=["POST"])
def add_stock_level():
    user_id: str = request.json.get("user_id")
    store_id: str = request.json.get("store_id")
    book_id: str = request.json.get("book_id")
    add_num: str = request.json.get("add_stock_level", 0)

    s = seller.Seller()
    code, message = s.add_stock_level(user_id, store_id, book_id, add_num)

    return jsonify({"message": message}), code

@bp_seller.route("/ship_order", methods=["POST"])
def ship_order():
    user_id = request.json.get("user_id")  # 卖家ID
    store_id = request.json.get("store_id")
    order_id = request.json.get("order_id")
    
    s = seller.Seller()
    code, message = s.ship_order(user_id, store_id, order_id)
    return jsonify({"message": message}), code

@bp_seller.route("/get_book_price_and_stock", methods=["GET"])
def get_book_price_and_stock():
    store_id = request.args.get("store_id")
    book_id = request.args.get("book_id")
    s = seller.Seller()
    code, result = s.get_book_price_and_stock(store_id, book_id)

    if code == 200:
        return jsonify({
            "stock_quantity": result["stock_quantity"],
            "book_price": result["book_price"],
        }), 200
    else:
        return jsonify({"message": result}), code

@bp_seller.route("/change_book_price", methods=["POST"])
def change_book_price():
    user_id: str = request.json.get("user_id")
    store_id: str = request.json.get("store_id")
    book_id: str = request.json.get("book_id")
    new_price: int = request.json.get("new_price")

    s = seller.Seller()
    code, message = s.change_book_price(user_id, store_id, book_id, new_price)

    return jsonify({"message": message}), code