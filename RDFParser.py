from typing import Any

from rdflib import Graph

from DataStorage import DataStorage
from typing import Optional

class RDFParser:

    def __init__(self, graph_url: str) -> None:
        self.graph = Graph()
        self.graph.parse(graph_url)
        self.mapped_values = {}

    def fill_data(self, db: DataStorage) -> None:
        for s, p, o in self.graph.triples((None, None, None)):
            db.insert_item('subject.db',  self.map_item(s), f"p: {p} - o: {o}")
            db.insert_item('predicate.db', self.map_item(p), f"o: {o} - s: {s}")
            db.insert_item('object.db', self.map_item(o), f"s: {s} - p: {p}")
            db.insert_item('subject-predicate.db', self.map_item(s, p), f"o: {o}")
            db.insert_item('predicate-object.db', self.map_item(p, o), f"s: {s}")
            db.insert_item('subject-object.db', self.map_item(s, o), f"p: {p}")
        db.save_all_db()

    def map_item(self, item1: Optional[Any] = None, item2: Optional[Any] = None) -> str:
        # if item in self.mapped_values.values():
        #     return list(self.mapped_values.keys())[list(self.mapped_values.values()).index(item)]

        item = f"{str(item1 or '')}{str(item2 or '')}"
        for key, value in self.mapped_values.items():
            if value == item:
                return str(key)
        mapped_id = len(self.mapped_values)
        self.mapped_values[mapped_id] = item
        return str(mapped_id)
