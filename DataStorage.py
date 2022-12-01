from typing import Union, List

import pickledb as pickledb

DATABASES_NAMES = ['subject.db',
                   'predicate.db',
                   'object.db',
                   'subject-predicate.db',
                   'subject-object.db',
                   'predicate-object.db']
DIRECTORY = 'databases/'


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

    def _set_item(self, db_name: str, key: str, value: str) -> None:
        self.databases[db_name].set(key, value)

    def get_item(self, db_name: str, key: str) -> Union[str, List[str]]:
        return self.databases[db_name].get(key)

    def _append_item(self, db_name: str, key: str, more: str) -> None:
        return self.databases[db_name].append(key, f"\n{more}")

    def insert_item(self, db_name: str, key: str, value: str) -> None:
        if not self.get_item(db_name, key):
            self._set_item(db_name, key, value)
        else:
            self._append_item(db_name, key, value)

    def get_keys(self, db_name) -> List[str]:
        return self.databases[db_name].getall()

    def sort_alphabetically(self) -> None:
        for db_name in DATABASES_NAMES:
            self.databases[db_name] = sorted(self.databases[db_name])

    def delete_all(self):
        for db_name in DATABASES_NAMES:
            self.databases[db_name].deldb()
            self.databases[db_name].dump()
