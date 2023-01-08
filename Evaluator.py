import time

from Database import Database
from RDFParser import RDFParser, BATCH_SIZE
from Utilities import Utilities


class Evaluator:

    def __init__(self, data_base: Database, rdf_parser: RDFParser):
        self.utilities = Utilities(data_base, rdf_parser)
        self.rdf_parser = rdf_parser
        self.data_base = data_base
        self.all_keys = {}
        self.db_names = ["subject", "predicate", "object", "subject-predicate",
                         "subject-object", "predicate-object", "subject-predicate-object"]
        self.init_sets()

    def init_sets(self):
        for name in self.db_names:
            self.all_keys[name] = set()

    def evaluate_pattern_type(self, all_keys: list, decode_output=True):
        start_time = time.time() * 1000
        num_added = 0
        output = []
        decoded_outputs = []
        for key in all_keys:
            if num_added == 0:
                self.data_base.start_pipeline()
            self.data_base.get_item(key)
            num_added += 1
            if num_added == BATCH_SIZE:
                output += self.data_base.execute_commands()
                num_added = 0
        output += self.data_base.execute_commands()
        if decode_output:
            decoded_outputs += [self.rdf_parser.decode_items(x) for x in output]
        end_time = time.time() * 1000
        return (end_time - start_time) / (len(all_keys))

    def add_key(self, db_name, key):
        self.all_keys[db_name].add(self.rdf_parser.build_key(db_name, key))

    def add_fake_key(self, db_name, key):
        self.all_keys[db_name].add(self.rdf_parser.build_key(db_name, f"[{key}]"))

    def create_evaluation_keys(self):
        for s, p, o in self.rdf_parser.graph:
            s = self.rdf_parser.encode_item(s.n3(self.rdf_parser.graph.namespace_manager))
            p = self.rdf_parser.encode_item(p.n3(self.rdf_parser.graph.namespace_manager))
            o = self.rdf_parser.encode_item(o.n3(self.rdf_parser.graph.namespace_manager))
            # Real keys
            self.add_key("subject", str(s))
            self.add_key("predicate", str(p))
            self.add_key("object", str(o))
            self.add_key("subject-predicate", self.rdf_parser.concatenate_items(s, p))
            self.add_key("subject-object", self.rdf_parser.concatenate_items(s, o))
            self.add_key("predicate-object", self.rdf_parser.concatenate_items(p, o))
            self.add_key("subject-predicate-object",
                         self.rdf_parser.concatenate_items(s, self.rdf_parser.concatenate_items(p, o)))

            # Fake keys
            self.add_fake_key("subject", str(s))
            self.add_fake_key("predicate", str(p))
            self.add_fake_key("object", str(o))
            self.add_fake_key("subject-predicate", self.rdf_parser.concatenate_items(s, p))
            self.add_fake_key("subject-object", self.rdf_parser.concatenate_items(s, o))
            self.add_fake_key("predicate-object", self.rdf_parser.concatenate_items(p, o))
            self.add_fake_key("subject-predicate-object",
                              self.rdf_parser.concatenate_items(s, self.rdf_parser.concatenate_items(p, o)))

    def evaluate_subject(self):
        return self.evaluate_pattern_type(self.all_keys["subject"])

    def evaluate_predicate(self):
        return self.evaluate_pattern_type(self.all_keys["predicate"])

    def evaluate_object(self):
        return self.evaluate_pattern_type(self.all_keys["object"])

    def evaluate_subject_object(self):
        return self.evaluate_pattern_type(self.all_keys["subject-object"])

    def evaluate_subject_predicate(self):
        return self.evaluate_pattern_type(self.all_keys["subject-predicate"])

    def evaluate_predicate_object(self):
        return self.evaluate_pattern_type(self.all_keys["predicate-object"])

    def evaluate_subject_predicate_object(self):
        return self.evaluate_pattern_type(self.all_keys["subject-predicate-object"], decode_output=False)

    def evaluate_all(self):
        self.create_evaluation_keys()
        res0 = self.evaluate_subject_predicate_object()
        print(f"The time needed to search on a pattern that has everything is {res0} ms")
        res1 = self.evaluate_predicate_object()
        print(f"The time needed to search on a pattern that only the subject is not known is {res1} ms")
        res2 = self.evaluate_subject_object()
        print(f"The time needed to search on a pattern that only the predicate is not known is {res2} ms")
        res3 = self.evaluate_subject_predicate()
        print(
            f"The time needed to search on a pattern that only the object not is known is {res3} ms")
        res4 = self.evaluate_predicate()
        print(
            f"The time needed to search on a pattern that its predicate is known is {res4} ms")
        res5 = self.evaluate_subject()
        print(
            f"The time needed to search on a pattern that its subject is known is {res5} ms")
        res6 = self.evaluate_object()
        print(
            f"The time needed to search on a pattern that its object is known is {res6} ms")

        print(
            f"The average time to search on a triple is {(res1 + res2 + res3 + res4 + res5 + res6) / 6} ms, the graph has {len(self.rdf_parser.graph)} triples")
