import re
from typing import Any, Tuple, List

from rdflib import Graph
from rdflib_hdt import HDTDocument


from DataStorage import DataStorage
from typing import Optional


class RDFParser:

    def __init__(self, graph_url: str) -> None:
        # self.store = HDTStore(graph_url)
        self.document = HDTDocument("graphs/watdiv.10M.hdt")
        self.graph = Graph()
        self.graph.parse(graph_url)
        self.mapped_values = {}
        self.values_dict = {}

    def get_all_triples(self):
        all_triples = []
        triples, cardinality = self.document.search((None, None, None))
        for s, p, o in self.graph:
            s_prefix = s.n3(self.graph.namespace_manager)
            p_prefix = p.n3(self.graph.namespace_manager)
            o_prefix = o.n3(self.graph.namespace_manager)
            all_triples.append((s_prefix, p_prefix, o_prefix))
        return all_triples

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

    @staticmethod
    def concatenate_items(value1: str, value2: str) -> str:
        return f"{value1} - {value2}"

    @staticmethod
    def _split_concatenated_items(items: str) -> List[str]:
        return items.split(" - ")

    def decode_item(self, item: str) -> str:
        decoded_item = ""
        items = self._split_concatenated_items(item)
        for item in items:
            prefix_constant = item.split(":")
            prefix = self.mapped_values[prefix_constant[0]]
            constant_value = self.mapped_values[prefix_constant[1]]
            decoded_item += f"{prefix}:{constant_value} "
        return decoded_item[:-1]

    def encode_var(self, var):
        var_value = var.n3(self.graph.namespace_manager)
        return self.encode_item(var_value)

    def fill_data(self, db: DataStorage) -> None:
        # bulk_subject = {}
        # bulk_predicate = {}
        # bulk_object = {}
        batch_size = 100000
        num_added = 0
        # db.start_bulk_add()
        triples, cardinality = self.document.search((None, None, None))
        for s, p, o in triples:
            # encoded_subject = self.encode_var(s)
            # encoded_predicate = self.encode_var(p)
            # encoded_object = self.encode_var(o)
            encoded_subject = str(s)
            encoded_predicate = str(p)
            encoded_object = str(o)

            # subject_value = s.n3(self.graph.namespace_manager)
            # encoded_subject = self.encode_item(subject_value)
            # predicate_value = p.n3(self.graph.namespace_manager)
            # encoded_predicate = self.encode_item(predicate_value)
            # object_value = o.n3(self.graph.namespace_manager)
            # encoded_object = self.encode_item(object_value)
            # if len(bulk_subject) < batch_size:
            #     bulk_subject[encoded_subject] = self.concatenate_items(encoded_predicate, encoded_object)
            #     bulk_predicate[encoded_predicate] = self.concatenate_items(encoded_subject, encoded_object)
            #     bulk_object[encoded_object] = self.concatenate_items(encoded_subject, encoded_predicate)
            # else:
            #     db.bulk_add_item(list(bulk_subject.keys()), list(bulk_subject.values()))
            #     db.bulk_add_item(list(bulk_predicate.keys()), list(bulk_predicate.values()))
            #     db.bulk_add_item(list(bulk_object.keys()), list(bulk_object.values()))
            #     bulk_subject = {}
            #     bulk_predicate = {}
            #     bulk_object = {}
            if num_added == 0:
                db.start_bulk_add()

            db.add_item(encoded_subject, self.concatenate_items(encoded_predicate, encoded_object))
            db.add_item(encoded_predicate, self.concatenate_items(encoded_subject, encoded_object))
            db.add_item(encoded_object, self.concatenate_items(encoded_subject, encoded_predicate))

            num_added += 1

            if num_added-1 == batch_size:
                db.send_all()
                num_added = 0
                print("batch sent")


            # db.insert_item('subject.db',
            #                encoded_subject,
            #                self.concatenate_items(predicate_value, object_value))
            # db.insert_item('predicate.db',
            #                encoded_predicate,
            #                self.concatenate_items(object_value, subject_value))
            # db.insert_item('object.db',
            #                encoded_object,
            #                self.concatenate_items(subject_value, predicate_value))
            # db.insert_item('subject-predicate.db',
            #                self.concatenate_items(encoded_subject, encoded_predicate),
            #                object_value)
            # db.insert_item('predicate-object.db',
            #                self.concatenate_items(encoded_object, encoded_predicate),
            #                subject_value)
            # db.insert_item('subject-object.db',
            #                self.concatenate_items(encoded_subject, encoded_object),
            #                predicate_value)
        # db.save_all_db()
        db.send_all()

