from typing import List

from rdflib import Graph

from Database import Database

BATCH_SIZE = 10000


class RDFParser:
    def __init__(self, graph_name: str, url):
        self.graph = Graph()
        # add format=ttl in case triples are read from file
        # self.graph.parse(url, format='ttl')
        self.graph_name = graph_name
        self.mapped_values = {}
        self.values_dict = {}

    @staticmethod
    def concatenate_items(value1: str, value2: str) -> str:
        return f"{value1} - {value2}"

    @staticmethod
    def _split_concatenated_items(items: str) -> List[str]:
        return items.split(" - ")

    def build_key(self, table_name, key):
        return f"{self.graph_name}:{table_name}:{key}"

    def encode_item(self, value: str = None) -> str:
        items = value.split(":")
        encoded_value = ""
        for item in items:
            try:
                encoded_value += self.values_dict[item]
            except KeyError:
                mapped_id = str(len(self.mapped_values))
                self.mapped_values[mapped_id] = item
                self.values_dict[item] = mapped_id
                encoded_value += mapped_id
            encoded_value += ":"
        return encoded_value[:-1]

    def decode_item(self, item: bytes) -> str:
        item = item.decode("utf-8")
        decoded_item = ""
        items = self._split_concatenated_items(item)
        for item in items:
            prefix_constant = item.split(":")
            try:
                prefix = self.mapped_values[prefix_constant[0]]
            except IndexError:
                prefix = ''
            try:
                constant_value = ':' + self.mapped_values[prefix_constant[1]]
            except IndexError:
                constant_value = ''
            decoded_item += f"{prefix}{constant_value} "
        return decoded_item[:-1]

    def decode_items(self, items: List[str]):
        return [self.decode_item(item) for item in items]

    def fill_data(self, db: Database) -> None:
        """
        @ToDo : see how it behaves on different values of batch_size
        @ToDo: see how it behaves on encoded values.
        :param db:
        :return:
        """

        num_added = 0
        num_batches = 0
        for s, p, o in self.graph:

            subject_pattern = self.encode_item(s.n3(self.graph.namespace_manager))
            predicate_pattern = self.encode_item(p.n3(self.graph.namespace_manager))
            object_pattern = self.encode_item(o.n3(self.graph.namespace_manager))
            if num_added == 0:
                db.start_pipeline()

            db.add_item(self.build_key("subject", subject_pattern),
                        self.concatenate_items(predicate_pattern, object_pattern))
            db.add_item(self.build_key("predicate", predicate_pattern),
                        self.concatenate_items(subject_pattern, object_pattern))
            db.add_item(self.build_key("object", object_pattern),
                        self.concatenate_items(subject_pattern, predicate_pattern))
            db.add_item(self.build_key("subject-predicate", self.concatenate_items(subject_pattern, predicate_pattern)),
                        object_pattern)
            db.add_item(self.build_key("subject-object", self.concatenate_items(subject_pattern, object_pattern)),
                        predicate_pattern)
            db.add_item(self.build_key("predicate-object", self.concatenate_items(predicate_pattern, object_pattern)),
                        subject_pattern)
            db.add_item(self.build_key("subject-predicate-object", self.concatenate_items(subject_pattern, self.concatenate_items(predicate_pattern, object_pattern))),
                        1)

            num_added += 1

            if num_added == BATCH_SIZE:
                db.execute_commands()
                num_added = 0
                num_batches += 1
                print(f"Batch sent: {num_batches}")

        db.execute_commands()
        self.save_all(db)

    def save_all(self, db):
        db.set_dict(self.build_key("ids", "mapped_values"), self.mapped_values)
        db.set_dict(self.build_key("ids", "values_dict"), self.values_dict)

    def load_all(self, db):
        self.mapped_values = db.get_dict(self.build_key("ids", "mapped_values"))
        self.values_dict = db.get_dict(self.build_key("ids", "values_dict"))
