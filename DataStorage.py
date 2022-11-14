import pickledb


class DataStorage:

    def __init__(self):
        self.databases = {}
        self.databases_names = ['subject.db', 'predicate.db', 'object.db', 'subject-predicate.db', 'subject-object.db', 'predicate-object.db']
        for db_name in self.databases_names:
            self.load_db(db_name, False)

    def load_db(self, db_name: str, dump: bool):
        directory = 'databases/'
        self.databases[db_name] = pickledb.load(f"{directory}{db_name}", dump)

    def save_db(self, db_name: str):
        self.databases[db_name].dump()

    def save_all(self):
        for db_name in self.databases.keys():
            self.save_db(db_name)

    def set_item(self, db_name: str, key: str, value: str):
        self.databases[db_name].set(key, value)

    def get_keys(self, db_name):
        return self.databases[db_name].getall()

    def get_item(self, db_name: str, key: str):
        return self.databases[db_name].get(key)

    def append(self, db_name: str, key: str, more: str):
        return self.databases[db_name].append(key, f"\n{more}")

    def insert(self, db_name: str, key: str, value: str):
        if not self.get_item(db_name, key):
            self.set_item(db_name, key, value)
        else:
            self.append(db_name, key, value)
