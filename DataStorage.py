from typing import Union, List

import pickledb as pickledb

DIRECTORY = 'databases/'

DATABASES_NAMES = ['subject.db',
                   'predicate.db',
                   'object.db',
                   'subject-predicate.db',
                   'subject-object.db',
                   'predicate-object.db',
                   'subject-predicate-object.db']


class DataStorage:

    def __init__(self):
        self.databases = {}
        for db_name in DATABASES_NAMES:
            self._load_db(db_name)

    def _load_db(self, db_name: str, dump=False) -> None:
        self.databases[db_name] = pickledb.load(f"{DIRECTORY}{db_name}", dump)

    def save_db(self, db_name: str) -> None:
        self.databases[db_name].dump()

    def save_all_db(self) -> None:
        for db_name in DATABASES_NAMES:
            self.save_db(db_name)

    def graph_exists(self, graph_name):
        does_exist = self.databases['subject.db'].get(f"0")

        return does_exist is not False and does_exist is not None

    def _set_item(self, db_name: str, key: str, value: str) -> None:
        self.databases[db_name].set(key, value)

    def get_item(self, db_name: str, key: str) -> Union[str, List[str]]:
        value = self.databases[db_name].get(key)
        return value if value else []

    def _append_item(self, db_name: str, key: str, more: str) -> None:
        return self.databases[db_name].append(key, more)

    def add_item(self, db_name: str, key: str, value: str) -> None:
        item = self.get_item(db_name, key)
        if not item:
            self._set_item(db_name, key, [value])
        else:
            item.append(value)
            self._set_item(db_name, key, item)

    def get_keys(self, db_name) -> List[str]:
        return self.databases[db_name].getall()

    def sort_alphabetically(self) -> None:
        for db_name in DATABASES_NAMES:
            self.databases[db_name] = sorted(self.databases[db_name])

    def delete_all(self):
        for db_name in DATABASES_NAMES:
            self.databases[db_name].deldb()
            self.databases[db_name].dump()
