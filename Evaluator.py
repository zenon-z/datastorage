import time

from Database import Database
from RDFParser import RDFParser, BATCH_SIZE
from Utilities import Utilities
import rdf_loader


class Evaluator:

    def __init__(self, db: Database, rdf_parser: RDFParser, encoded_triples: list(), graph_name: str, rdf_loader_obj):
        self.utilities = Utilities(db, rdf_parser)
        self.rdf_parser = rdf_parser
        self.rdf_loader_obj = rdf_loader_obj
        self.graph_name = graph_name
        self.encoded_triples = encoded_triples
        self.db = db
        self.all_keys = {}
        self.db_names = ["subject", "predicate", "object", "subject-predicate",
                         "subject-object", "predicate-object", "subject-predicate-object"]
        self.init_sets()

    def init_sets(self):
        for name in self.db_names:
            self.all_keys[name] = set()

    def evaluate_pattern_type(self, db_name: str, all_keys: list, decode_output=True):
        start_time = time.time() * 1000
        output = []
        for key in all_keys:
            output.append(self.db.get_item(db_name, key))

        if decode_output:
            decoded_outputs = [self.rdf_loader_obj.decode_items(x) for x in output]
        end_time = time.time() * 1000
        return (end_time - start_time) / (len(all_keys))

    def add_key(self, db_name, key):
        self.all_keys[db_name].add(rdf_loader.build_key(self.graph_name, db_name, key))

    def add_fake_key(self, db_name, key):
        self.all_keys[db_name].add(rdf_loader.build_key(self.graph_name, db_name, f"[{key}]"))

    def create_evaluation_keys(self):
        for s, p, o in self.encoded_triples:
            # Real keys
            self.add_key("subject", str(s))
            self.add_key("predicate", str(p))
            self.add_key("object", str(o))
            self.add_key("subject-predicate", (s, p))
            self.add_key("subject-object", (s, o))
            self.add_key("predicate-object", (p, o))
            self.add_key("subject-predicate-object", (s, p, o))

            # Fake keys
            self.add_fake_key("subject", str(s))
            self.add_fake_key("predicate", str(p))
            self.add_fake_key("object", str(o))
            self.add_fake_key("subject-predicate", (s, p))
            self.add_fake_key("subject-object", (s, o))
            self.add_fake_key("predicate-object", (p, o))
            self.add_fake_key("subject-predicate-object", (s, p, o))

    def evaluate_subject(self):
        return self.evaluate_pattern_type("subject.db", self.all_keys["subject"])

    def evaluate_predicate(self):
        return self.evaluate_pattern_type("predicate.db", self.all_keys["predicate"])

    def evaluate_object(self):
        return self.evaluate_pattern_type("object.db", self.all_keys["object"])

    def evaluate_subject_object(self):
        return self.evaluate_pattern_type("subject-object.db", self.all_keys["subject-object"])

    def evaluate_subject_predicate(self):
        return self.evaluate_pattern_type("subject-predicate.db", self.all_keys["subject-predicate"])

    def evaluate_predicate_object(self):
        return self.evaluate_pattern_type("predicate-object.db", self.all_keys["predicate-object"])

    def evaluate_subject_predicate_object(self):
        return self.evaluate_pattern_type("subject-predicate-object.db", self.all_keys["subject-predicate-object"], decode_output=False)

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
            f"The average time to search on a triple is {(res1 + res2 + res3 + res4 + res5 + res6) / 6} ms, the graph has {len(self.encoded_triples)} triples")
