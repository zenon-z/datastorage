import redis
import json

class Database:

    def __init__(self, host, port, db_num):
        self.db = redis.StrictRedis(host=host, port=port, db=db_num)
        # self.db.flushdb()
        self.pipe = self.db

    def start_pipeline(self):
        self.pipe = self.db.pipeline()

    def execute_commands(self):
        if self.pipe != self.db:
            results = self.pipe.execute()
            self.pipe = self.db
            return results

    def add_item(self, key, value):
        self.pipe.lpush(key, value)

    def get_item(self, key):
        return self.pipe.lrange(key, 0, -1)

    def delete_item(self, key, value):
        self.db.lrem(key, 0, value)

    def graph_exists(self, graph_name):
        does_exist = self.db.exists(f"{graph_name}:subject:0:1")
        return does_exist == 1

    def set_dict(self, key, value):
        hashed_value = json.dumps(value)
        self.db.set(key, hashed_value)

    def get_dict(self, key):
        hashed_value = self.db.get(key)
        return json.loads(hashed_value)

