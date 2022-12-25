from typing import List
from rdflib import Graph
from rdflib_hdt import HDTDocument, HDTStore
from Database import Database
import time


class RDFParser:

    def __init__(self, graph_url: str) -> None:
        url = "graphs/output.ttl"
        # self.store = HDTStore("graphs/watdiv.10M.hdt")
        # self.document = HDTDocument("graphs/watdiv.10M.hdt")
        # self.graph = Graph(store=self.store)
        #
        # self.graph.serialize(format="xml", destination="graphs/output.xml")
        # self.save_hdt_to_ttl_file()
        #
        # self.triples, self.cardinality = self.document.search((None, None, None))
        self.graph = Graph()
        self.graph_name = "watdiv"
        parse_start_time = time.time()
        self.graph.parse("graphs/output.ttl")

        self.triples = self.graph
        parse_end_time = time.time()
        a = parse_end_time - parse_start_time
        print("parsing time is ", a)
        # self.graph.serialize(format="ttl", destination="graphs/dbpedia.ttl")
        self.mapped_values = {}
        self.values_dict = {}

    def save_hdt_to_ttl_file(self):
        v = self.graph.serialize(format="ttl")

    def get_all_triples(self):
        all_triples = []

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

    def fill_data(self, db: Database) -> None:
        """
        @ToDo : see how it behaves on different values of batch_size
        @ToDo: see how it behaves on encoded values.
        :param db:
        :return:
        """
        batch_size = 100000
        num_added = 0

        for s, p, o in self.triples:
            # encoded_subject = self.encode_var(s)
            # encoded_predicate = self.encode_var(p)
            # encoded_object = self.encode_var(o)
            subject_pattern = str(s)
            predicate_pattern = str(p)
            object_pattern = str(o)

            if num_added == 0:
                db.start_bulk_add()

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
                        self.concatenate_items(subject_pattern, self.concatenate_items(predicate_pattern, object_pattern)))

            num_added += 1

            if num_added - 1 == batch_size:
                db.send_all()
                num_added = 0
                # print("batch sent")

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

    def build_key(self, table_name, key):
        return f"{self.graph_name}:{table_name}:{key}"
