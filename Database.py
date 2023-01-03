import redis


class Database:

    def __init__(self, host, port, db_num):
        self.db = redis.StrictRedis(host=host, port=port, db=db_num)
        self.db.flushdb()
        self.pipe = self.db

    def start_pipeline(self):
        self.pipe = self.db.pipeline()

    def execute_commands(self):
        self.pipe.execute()
        self.pipe = self.db

    def add_item(self, key, value):
        self.pipe.lpush(key, value)

    def get_item(self, key):
        return self.pipe.lrange(key, 0, -1)

    def delete_item(self, key, value):
        self.db.lrem(key, 0, value)
