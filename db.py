'''
This is an abstraction module to handle any DB connection

For now we are using SQLite as it is self-contained and does
not require any external software installed

For this toy application maybe this is not needed since we do not
need any persisten storage?

For now the structure of this DB is going to be the SIMPLEST POSSIBLE:
TABLE users:
 username (TEXT) PK

TABLE rooms:
 room (TEXT) | username (TEXT) both PK
This means that a room does not exist in the database up until it has messages in it
All the membership is done server side FIXME

TABLE joined_rooms:
 roomname (TEXT) FK | username (TEXT) FK
This table shows which rooms has a user joined

TABLE messages:
 offset (INTEGER) | room (TEXT) | username (TEXT) | message (TEXT) | timestamp (DATETIME)
This table stores every message in a room, with its corresponding offset, username, room and timestamp
'''
import sqlite3 as sqlite
from collections import namedtuple
from datetime import datetime

# named tuple as message?
# Fields: (offset room PK) user content timestamp
Message = namedtuple('Message', ['offset', 'roomname', 'username', 'content', 'timestamp'])

class DB:
    def __init__(self):
        self.con = sqlite.connect('db/testdb.db',
                detect_types=sqlite.PARSE_DECLTYPES, # To use TIMESTAMP as type later (has to be a datetime.datetime object
                check_same_thread=False)             # Share connection between Threads
        # Create test table
        cur = self.con.cursor()
        # TODO: create REAL persistence
        cur.execute('''CREATE TABLE IF NOT EXISTS users
                       (name text PRIMARY KEY)''')
        cur.execute('''INSERT OR IGNORE INTO users (name)
                       VALUES ('alvaroc'), ('test_user')''')


        cur.execute('''CREATE TABLE IF NOT EXISTS rooms
                       (name text PRIMARY KEY)''')
        cur.execute('''INSERT OR IGNORE INTO rooms (name)
                       VALUES
                       ('welcome'),
                       ('random')''')

        # Declare FOREIGN KEYs so we can avoid doing extra checks server-side
        # This may cause the queries to fail? ugh...
        cur.execute('''CREATE TABLE IF NOT EXISTS joined_rooms
                       (username TEXT,
                        roomname TEXT,
                        PRIMARY KEY (username, roomname),
                        FOREIGN KEY (username) REFERENCES users (name),
                        FOREIGN KEY (roomname) REFERENCES rooms (name));''')
        cur.execute('''INSERT OR IGNORE INTO joined_rooms (username, roomname)
                       VALUES
                       ('alvaroc', 'welcome'),
                       ('test_user', 'welcome')''')
        cur.execute('''CREATE TABLE IF NOT EXISTS messages
                       (offset INTEGER,
                        roomname TEXT,
                        username TEXT,
                        content  TEXT,
                        timestamp TIMESTAMP,
                        PRIMARY KEY (offset, roomname),
                        FOREIGN KEY (username) REFERENCES users(name),
                        FOREIGN KEY (roomname) REFERENCES rooms(name))''')
        self.con.commit()

    def test(self):
        cur = self.con.cursor()
        cur.execute('SELECT SQLITE_VERSION()')
        data = cur.fetchone()[0]
        print('SQLite version:', data)

    def get_joined_rooms(self, user):
        ''' Returns all the rooms the user has joined'''
        cur = self.con.cursor()
        cur.execute('''SELECT roomname
                       FROM joined_rooms
                       WHERE username = ?
                    ''', user)
        rooms = [room for room, _ in cur.fetchall()]
        return rooms

    ### Users ###
    def get_users(self):
        ''' Returns all the users from the DB'''
        cur = self.con.cursor()
        cur.execute('''SELECT * FROM users''')
        # cur.fetchall returns a tuple of the same shape as the table shape
        # in this case (name,)
        users = [user for user, in cur.fetchall()]
        return users

    def create_user(self, user):
        '''Create a new user'''
        print(f'Create user received {user} as user')
        cur = self.con.cursor()
        try:
            cur.execute('''INSERT INTO users (name)
                       VALUES (?)''', (user,))
            self.con.commit()
        except sqlite.Error as e:
            print(f'Error creating user {user}: {e}')
            return False
        return True

    def delete_user(self, user):
        '''Delete a user from the database'''
        cur = self.con.cursor()
        try:
            cur.execute('''DELETE FROM users WHERE name = ?''', (user,))
            self.con.commit() # Not needed? No time to check
        except sqlite.Error as e:
            print(f'Error deleting user {user}: {e}')
            return False
        return True


    ### Rooms ###
    def get_rooms(self):
        ''' Returns all the available rooms'''
        cur = self.con.cursor()
        cur.execute('''SELECT * FROM rooms''')
        rooms = [room for room, in cur.fetchall()]
        return rooms

    def create_room(self, room):
        '''Create a new room'''
        print(f'Create room received {room} as room')
        cur = self.con.cursor()
        try:
            cur.execute('''INSERT INTO rooms (name)
                       VALUES (?)''', (room,))
            self.con.commit()
        except sqlite.Error as e:
            print(f'Error creating room {room}: {e}')
            return False
        return True

    def delete_room(self, room):
        '''Delete a room from the database'''
        cur = self.con.cursor()
        try:
            cur.execute('''DELETE FROM rooms WHERE name = ?''', (room,))
            self.con.commit() # Not needed? No time to check
        except sqlite.Error as e:
            print(f'Error deleting room {room}: {e}')

    def get_joined_users(self, room):
        ''' gets all the users in the room '''
        cur = self.con.cursor()
        try:
            cur.execute('''SELECT username AS name
                           FROM joined_rooms
                           WHERE roomname = ?''', (room,))
            users = [user for user, in cur.fetchall()]
            return users
        except sqlite.Error as e:
            print(f'Error fetching users for room: {room}: {e}')
            return []

    ### Joined Rooms ###
    def get_joined_rooms(self, user):
        ''' gets all the rooms this user has joined '''
        cur = self.con.cursor()
        try:
            cur.execute('''SELECT roomname AS name
                           FROM joined_rooms
                           WHERE username = ?''', (user,))
            rooms = [room for room, in cur.fetchall()]
            return rooms
        except sqlite.Error as e:
            print(f'Error fetching joined rooms from user {user}: {e}')
            return []

    def join_room(self, user, room):
        '''Joins the user to the room'''
        cur = self.con.cursor()
        try:
            cur.execute('''INSERT INTO joined_rooms (username, roomname)
                       VALUES (?, ?)''', (user, room))
            self.con.commit()
        except sqlite.Error as e:
            print(f'Error joining user {user} into room {room}: {e}')
            return False
        return True

    def leave_room(self, user, room):
        '''Removes the user from the room'''
        cur = self.con.cursor()
        try:
            cur.execute('''DELETE FROM joined_rooms
                           WHERE username = ? AND roomname = ?''', (user, room))
            self.con.commit()
        except sqlite.Error as e:
            print(f'Error deleting user {user} from room {room}: {e}')
            return False
        return True

    ### Messages ### 
    def get_room_messages(self, room):
        ''' Gets all the stored messages from this room '''
        cur = self.con.cursor()
        cur.execute('''SELECT *
                       FROM messages
                       WHERE roomname = ?''',(room,))
        messages = [Message(offset=offset, roomname=roomname, username=username, 
                            content=content, timestamp=ts) \
                    for offset, roomname, username, content, ts in cur.fetchall()]
        return messages

    def send_message(self, message):
        ''' Send a message '''
        cur = self.con.cursor()
        try:
            cur.execute('''INSERT INTO messages (offset, roomname, username, content, timestamp)
                       VALUES (?, ?, ?, ?, ?)''', (message.offset, message.roomname,
                                                   message.username, message.content,
                                                   message.timestamp))
            self.con.commit()
        except sqlite.Error as e:
            print(f'Error sending message {message} in room {message.roomname}: {e}')
            return -1
        return True
    def close(self):
        self.con.close()


db = None
def init_db():
    '''
    this function creates the SQLite instance
    '''
    db = DB()
    db.test()
    print(db.get_users())
    print(db.get_rooms())
    db.close()

# Test
if __name__ == '__main__':
    init_db()
