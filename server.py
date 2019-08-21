#!/usr/bin/env python
'''
This is the server running in a daemon process in a remote server
This is an example of a python program

NOTE: This is not optimized whatsoever, it's just for teaching purposes
This resembles a REST API, but does not necessarily adhere to the spec, 
this is quick and dirty
'''
from flask import Flask, jsonify, abort, make_response, escape
from datetime import datetime
from db import DB
app = Flask(__name__)

UNAUTHORIZED_ERROR = 401
NOT_FOUND_ERROR = 404
ALREADY_EXISTS_ERROR = 409

# Global objects
db = DB()
cached_users = None
cached_rooms = None
cached_messages = None

def init_cache():
    ''' Initialize the cache '''
    global cached_users
    global cached_rooms
    # global cached_messages
    cached_users = set(db.get_users())
    cached_rooms = set(db.get_rooms())

@app.route("/")
def main():
    return "These are not the messages you are looking for..."

@app.route("/admin")
def unauthorized(arg):
    print(arg)
    print(dir(arg))
    abort(UNAUTHORIZED_ERROR)

@app.route("/test", methods=['GET'])
def json_test():
    return jsonify({'test': {
        'test1': ['1', '2', '3'],
        'test2': 123,
        'test3': "Hello"}
        })

### Users ###
@app.route("/user/list")
def list_users():
    ''' List all the existing users '''
    users = [{'user': user} for user in cached_users]
    return jsonify(users)

@app.route("/user/create/<username>")
def create_user(username, methods=['POST']):
    ''' Creates the user received as a parameter in the URL '''
    if username in cached_users:
        abort(ALREADY_EXISTS_ERROR) # Already Exists
    elif not db.create_user(username):
        abort(ALREADY_EXISTS_ERROR) # Probably bad, 'cause other error could occur, whatever...
    else:
        cached_users.add(username)  # Crated user, store it in the local cache
        return jsonify({'user': username })

@app.route("/user/delete/<username>")
def delete_user(username):
    ''' Deletes the username from the database '''
    # TODO: Include some "protection" (like a "secret" code) in a URL param against trolls 
    # trying to delete each other's accounts
    if username in cached_users:
        if not db.delete_user(username):
            abort(NOT_FOUND_ERROR)
        else:
            cached_users.remove(username)
    else:
        abort(NOT_FOUND_ERROR)
        
### Rooms ###
@app.route("/room/list")
def list_rooms():
    ''' List all the existing rooms '''
    rooms = [{'room': room} for room in cached_rooms]
    return jsonify(rooms)

@app.route("/room/create/<roomname>")
def create_room(roomname, methods=['POST']):
    ''' Creates the room received as a parameter in the URL '''
    if roomname in cached_rooms:
        abort(ALREADY_EXISTS_ERROR) # Already Exists
    elif not db.create_room(roomname):
        abort(ALREADY_EXISTS_ERROR) # Probably bad, 'cause other error could occur, whatever...
    else:
        cached_rooms.add(roomname)  # Crated user, store it in the local cache
        return jsonify({'room': roomname})

@app.route("/room/delete/<roomname>")
def delete_room(roomname):
    ''' Deletes the room from the database '''
    # TODO: Some logic to make sure the room is safe to delete?
    if roomname in cached_rooms:
        if not db.delete_room(roomname):
            abort(NOT_FOUND_ERROR)
        else:
            cached_rooms.remove(roomname)
    else:
        abort(NOT_FOUND_ERROR)

### Error Handlers ###
@app.errorhandler(NOT_FOUND_ERROR)
def error_not_found(error):
    return make_response(jsonify({'error': 'Not found'}), NOT_FOUND_ERROR)

@app.errorhandler(ALREADY_EXISTS_ERROR)
def error_already_exists(error):
    return make_response(jsonify({'error': 'Already Exists'}), ALREADY_EXISTS_ERROR)

### Init ###
def init_server():
    ''' This initializes the server to bootstrap the DB and some basic data '''

if __name__ == '__main__':
    print("Initializing the cache...")
    init_cache()
    app.run(debug=True)
