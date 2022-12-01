from typing import Any

from rdflib import Graph

from DataStorage import DataStorage
from typing import Optional


class RDFParser:

    def __init__(self, graph_url: str) -> None:
        self.graph = Graph()
        self.graph.parse(graph_url)
        self.mapped_values = {}
        self.values_dict = {}

    def fill_data(self, db: DataStorage) -> None:
        for s, p, o in self.graph:
            s_prefix = s.n3(self.graph.namespace_manager)
            p_prefix = p.n3(self.graph.namespace_manager)
            o_prefix = o.n3(self.graph.namespace_manager)
            db.insert_item('subject.db', self.map_item(s_prefix), f"?p= {p_prefix} , ?o= {o_prefix}")
            db.insert_item('predicate.db', self.map_item(p_prefix), f"?o= {o_prefix} , ?s= {s_prefix}")
            db.insert_item('object.db', self.map_item(o_prefix), f"?s= {s_prefix} , ?p= {p_prefix}")
            db.insert_item('subject-predicate.db', self.map_item(s_prefix, p_prefix), f"?o= {o_prefix}")
            db.insert_item('predicate-object.db', self.map_item(p_prefix, o_prefix), f"?s= {s_prefix}")
            db.insert_item('subject-object.db', self.map_item(s_prefix, o_prefix), f"?p= {p_prefix}")
        db.save_all_db()

    def map_item_long(self, item1: Optional[Any] = None, item2: Optional[Any] = None) -> str:
        item = f"{str(item1 or '')}{str(item2 or '')}"
        if item in self.values_dict:
            for key, value in self.mapped_values.items():
                if value == item:
                    return str(key)
        else:
            mapped_id = len(self.mapped_values)
            self.mapped_values[mapped_id] = item
            self.values_dict[item] = 1
            return str(mapped_id)

    def map_item(self, item1: Optional[Any] = None, item2: Optional[Any] = None) -> str:
        item = f"{str(item1 or '')}{str(item2 or '')}"
        if item in self.values_dict.keys():
            return self.values_dict[item]
        else:
            mapped_id = len(self.mapped_values)
            self.mapped_values[mapped_id] = item
            self.values_dict[item] = str(mapped_id)
            return str(mapped_id)
