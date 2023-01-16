import time

from Database import Database
from RDFParser import RDFParser, BATCH_SIZE
from Utilities import Utilities
import rdf_loader
import random_triple_generator


class Evaluator:

    def __init__(self, db: Database, encoded_triples: list(), graph_name: str, rdf_loader_obj):
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
        print(f"Evaluation of {db_name}")
        start_time = time.time() * 1000
        output = []
        for key in all_keys:
            output.append(self.db.get_item(db_name, key))

        if decode_output:
            decoded_outputs = [self.rdf_loader_obj.decode_items(x) for x in output]
        end_time = time.time() * 1000

        num_keys = len(all_keys)
        all_keys.clear()
        return (end_time - start_time) / num_keys

    def add_key(self, db_name, key):
        self.all_keys[db_name].add(key)

    def add_fake_key(self, db_name, key):
        self.all_keys[db_name].add(rdf_loader.build_key(f"[{key}]"))

    def get_keys(self):
        random_keys = random_triple_generator.get_random_triples(f"{self.graph_name}.ttl")
        for key in random_keys.keys():
            for line in random_keys[key]:
                triple = line.split(" ")
                s = triple[0]
                p = triple[1]
                o = triple[2]
                self.add_key("subject", f"{s}")
                self.add_key("predicate", f"{p}")
                self.add_key("object", f"{o}")
                self.add_key("subject-predicate", f"{s}{p}")
                self.add_key("subject-object", f"{s}{o}")
                self.add_key("predicate-object", f"{p}{o}")
                self.add_key("subject-predicate-object", f"{s}{p}{o}")

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
        return self.evaluate_pattern_type("subject-predicate-object.db", self.all_keys["subject-predicate-object"],
                                          decode_output=False)

    def evaluate_all(self):
        # self.create_evaluation_keys()
        self.get_keys()
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
