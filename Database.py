import redis


class Database:

    def __init__(self, host, port, db_num):
        self.db = redis.Redis(host=host, port=port, db=db_num)
        self.db.flushdb()
        self.pipe = self.db

    def add_item(self, key, value):
        self.pipe.rpush(key, value)

    def start_bulk_add(self):
        self.pipe = self.db.pipeline()

    def send_all(self):
        if self.pipe:
            self.pipe.execute()
            self.pipe = self.db

    def get_item(self, key):
        return self.db.lrange(key, 0, -1)
