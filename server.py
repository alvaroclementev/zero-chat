#!/usr/bin/env python
'''
This is the server running in a daemon process in a remote server
This is an example of a python program

NOTE: This is not optimized whatsoever, it's just for teaching purposes
This resembles a REST API, but does not necessarily adhere to the spec,
this is quick and dirty
'''
from flask import Flask, jsonify, request, abort, make_response, escape
# from collections import namedtuple
from datetime import datetime
from db import DB, Message
app = Flask(__name__)

UNAUTHORIZED_ERROR = 401
NOT_FOUND_ERROR = 404
ALREADY_EXISTS_ERROR = 409
UNPROCESSABLE_ENTITY_ERROR = 422
INTERNAL_SERVER_ERROR = 500


# Global objects
# FIXME: Use an Application Context here to thread safely use these caches
db = DB()
cached_users = None    # Just a set with all the users for quick membership check
cached_rooms = None    # A dict with rooms as keys and a set of joined users as values
cached_messages = None # A dict with rooms as keys and list of messages as values
room_offsets = None    # A dict with the last offset stored in the room

def init_cache():
    ''' Initialize the cache '''
    global cached_users
    global cached_rooms
    global cached_messages
    global room_offsets
    cached_users = set(db.get_users())
    cached_messages = {}
    room_offsets = {}

    # Init the rooms cache
    cached_rooms = {room: set() for room in db.get_rooms()}
    for room in cached_rooms: # FIXME: This can be done faster directly in a SQL Join
        joined_users = db.get_joined_users(room)
        for username in joined_users:
            if username in cached_users:
                cached_rooms[room].add(username)
            else:
                # WTF should not happen, since it's forced by FK?
                print('INIT ERROR: Joined user not in Cached Users?')
                return False
        # Init the messages cache
        messages = db.get_room_messages(room)
        cached_messages[room] = messages
        room_offsets[room] = max([message.offset for message in messages]) if messages else -1
    return True


@app.route("/")
def root():
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

@app.route("/user/create/<username>", methods=['POST'])
def create_user(username):
    ''' Creates the user received as a parameter in the URL '''
    if username in cached_users:
        abort(ALREADY_EXISTS_ERROR) # Already Exists
    elif not db.create_user(username):
        abort(ALREADY_EXISTS_ERROR) # Probably bad, 'cause other error could occur, whatever...
    else:
        # Created user, store it in the local cache
        cached_users.add(username)
        # Add the user to the welcome room
        db.join_room(username, 'welcome')
        cached_rooms['welcome'].add(username)
        return jsonify({"username": username, "joined_rooms": ['welcome']})

@app.route("/user/delete/<username>")
def delete_user(username):
    ''' Deletes the username from the database '''
    # TODO: Include some "protection" (like a "secret" code) in a URL param against trolls
    # trying to delete each other's accounts
    if username in cached_users:
        if not db.delete_user(username):
            abort(NOT_FOUND_ERROR)
        else:
            # Remove from all the joined rooms
            joined_rooms = db.get_joined_rooms(username)
            for room in joined_rooms:
                if room in cached_rooms:
                    cached_rooms[room].remove(username)
                db.leave_room(username, room)
            # Remove user from the cache
            cached_users.remove(username)
    else:
        abort(NOT_FOUND_ERROR)

### Joined Rooms ###
@app.route("/joined_room/join/<roomname>")
def join_room(roomname):
    ''' Join the room '''
    # Make sure the user exists
    username = None
    if 'username' not in request.args:
        abort(UNPROCESSABLE_ENTITY_ERROR) # TODO: which one was the one that returned 404?
    else:
        username = request.args.get('username')
        if username not in cached_users:
            abort(NOT_FOUND_ERROR)
    # Now check the room
    if roomname not in cached_rooms:
        abort(NOT_FOUND_ERROR)
    else:
        success = db.join_room(username, roomname)
        if not success:
            abort(INTERNAL_SERVER_ERROR)
        else:
            # All well! Return all joined rooms from this user
            cached_rooms[roomname].add(username)
            rooms = db.get_joined_rooms(username)
            return jsonify({ 'user': username, 'rooms': rooms })

@app.route("/joined_room/list/<username>")
def list_joined_rooms(username):
    ''' Join the room '''
    # Make sure the user exists
    if username not in cached_users:
        abort(NOT_FOUND_ERROR)
    else:
        rooms = db.get_joined_rooms(username)
        return jsonify({ 'user': username, 'rooms': rooms })

@app.route("/joined_room/leave/<roomname>")
def leave_room(roomname):
    # Make sure the user exists
    username = None
    if 'username' not in request.args:
        abort(UNPROCESSABLE_ENTITY_ERROR) # TODO: which one was the one that returned 404?
    else:
        username = request.args.get('username')
        if username not in cached_users:
            abort(NOT_FOUND_ERROR)
    # Now check the room
    if roomname not in cached_rooms:
        abort(NOT_FOUND_ERROR)
    elif username not in cached_rooms[roomname]:
        abort(NOT_FOUND_ERROR) # FIXME: Not the best error code, at all!
    else:
        success = db.leave_room(username, roomname)
        if not success:
            abort(INTERNAL_SERVER_ERROR)
        else:
            # All well! Return all joined rooms from this user
            cached_rooms[roomname].remove(username)
            rooms = db.get_joined_rooms(username)
            return jsonify({ 'user': username, 'rooms': rooms })

@app.route("/joined_room/all")
def list_all_rooms():
    rooms = [{room: list(users)} for room, users in cached_rooms.items()]
    return jsonify(rooms)

### Rooms ###
@app.route("/room/list")
def list_rooms():
    ''' List all the existing rooms '''
    rooms = [{'room': room} for room in cached_rooms]
    return jsonify(rooms)

@app.route("/room/create/<roomname>", methods=['POST'])
def create_room(roomname,):
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

### Messages ###
@app.route("/message/get/<roomname>")
def get_messages(roomname):
    ''' Gets all the messages from a room '''
    # FIXME: Migrate all of this to WebSocket so we can push to the client directly?
    if roomname not in cached_rooms or roomname not in cached_messages:
        abort(NOT_FOUND_ERROR)
    else:
        messages = cached_messages[roomname]  # TODO: Think about how we refresh this cache... it should be kept in sync by us manually, no need to call back to the db
        json_messages = [{'offset': message.offset, 'username': message.username, 'content': message.content, 'timestamp': int(datetime.timestamp(message.timestamp)) } for message in messages]
        return jsonify({ 'roomname': roomname, 'messages': json_messages})

@app.route("/message/send/<roomname>", methods=['GET', 'POST'])
def send_message(roomname):
    ''' Send a message '''
    message = request.json["message"]
    if "username" not in message and \
       "roomname" not in message and \
       "content" not in message and \
       "timestamp" not in message:
           abort(UNPROCESSABLE_ENTITY_ERROR)
    elif message["roomname"] not in cached_rooms or \
         message["username"] not in cached_users:
             abort(NOT_FOUND_ERROR)
    elif message["username"] not in cached_rooms[message["roomname"]]:
        abort(UNAUTHORIZED_ERROR)
    else:
        # Well formed message, send it!
        timestamp = datetime.fromtimestamp(message["timestamp"])
        this_offset = room_offsets[message["roomname"]] + 1
        sent_message = Message(offset=this_offset, roomname=message["roomname"],
                               username=message["username"], content=message["content"],
                               timestamp=timestamp)
        print(f'Trying to send message {message} and the offset is {room_offsets[message["roomname"]]}')
        if not db.send_message(sent_message):
            abort(INTERNAL_SERVER_ERROR)
        else:
            # Successfully saved the message to the db
            room_offsets[message["roomname"]] = this_offset
            cached_messages[message["roomname"]].append(sent_message)

    return jsonify({'status': 200}) # What to send back here?
# TODO: Think about a function that returns the *NEW* messages (maybe using the offset variable, since it is a autoincrementing INTEGER?)


### Error Handlers ###
@app.errorhandler(NOT_FOUND_ERROR)
def error_not_found(error):
    return make_response(jsonify({'code': error.code, 'description': error.description}), NOT_FOUND_ERROR)

@app.errorhandler(ALREADY_EXISTS_ERROR)
def error_already_exists(error):
    return make_response(jsonify({'code': error.code, 'description': error.description}), ALREADY_EXISTS_ERROR)

@app.errorhandler(INTERNAL_SERVER_ERROR)
def error_already_exists(error):
    return make_response(jsonify({'code': error.code, 'description': error.description}), INTERNAL_SERVER_ERROR)

@app.errorhandler(UNAUTHORIZED_ERROR)
def error_unauthorized(error):
    return make_response(jsonify({'code': error.code, 'description': error.description}), UNAUTHORIZED_ERROR)

### Init ###
def init_server():
    ''' This initializes the server to bootstrap the DB and some basic data '''
    pass

if __name__ == '__main__':
    print("Initializing the cache...")
    if not init_cache():
        print('ERROR INITIALIZING CACHE...')
        exit(1)
    app.run(debug=True)
