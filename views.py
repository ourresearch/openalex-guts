from functools import wraps

from flask import jsonify, request

from app import app, GUTS_API_KEY
from util import normalize_openalex_id


@app.route("/")
def index():
    return jsonify(
        {
            "version": "0.1",
            "documentation_url": "/docs",
            "msg": "Don't panic",
        }
    )


def require_api_key(view_function):
    @wraps(view_function)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('x-api-key')
        if not api_key or api_key != GUTS_API_KEY:
            return jsonify({"message": "Invalid or missing API Key"}), 403
        return view_function(*args, **kwargs)
    return decorated_function


@app.route('/claim-author', methods=['POST'])
@require_api_key
def claim_author():
    """
    Associates a user id from the frontend with an openalex author ID.
    """
    data = request.get_json()
    if not data:
        return jsonify({'message': 'Invalid request: no json was sent with the request'}), 400

    user_id = data.get('user_id')
    openalex_author_id = normalize_openalex_id(data.get('openalex_author_id'))

    if not user_id or not openalex_author_id:
        return jsonify({'message': 'Invalid request: user_id and openalex_author_id are required'}), 400

    # logic goes here
    # create user_id and openalex_author_id pair if it does not already exist

    message = f"success: user_id {user_id} associated with openalex_author_id {openalex_author_id}"
    return jsonify({'message': message}), 200


@app.route('/merge-authors', methods=['POST'])
@require_api_key
def merge_authors():
    """
    Merge openalex author IDs into a claimed profile.
    """
    data = request.get_json()
    if not data:
        return jsonify({'message': 'Invalid request: no json was sent with the request'}), 400

    target_id = data.get('target_id')
    ids_to_merge = data.get('ids_to_merge')

    if not target_id or not ids_to_merge:
        return jsonify({'message': 'Invalid request: target_id and ids_to_merge are required'}), 400

    # logic goes here
    # check if target_id is claimed
    # check if ids_to_merge are claimed
    # merge ids_to_merge into target_id

    message = f"success: merged {ids_to_merge} into {target_id}"
    return jsonify({'message': message}), 200
