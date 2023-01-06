import time

from Database import Database
from RDFParser import RDFParser, BATCH_SIZE
from Utilities import Utilities


class Evaluator:

    def __init__(self, data_base: Database, rdf_parser: RDFParser):
        self.utilities = Utilities(data_base, rdf_parser)
        self.rdf_parser = rdf_parser
        self.data_base = data_base

    def evaluate_pattern_type(self, all_keys: list):
        start_time = time.time() * 1000
        num_added = 0
        output = []
        decoded_outputs = []
        all_keys = set(all_keys)
        for key in all_keys:
            if num_added == 0:
                self.data_base.start_pipeline()
            self.data_base.get_item(key)
            num_added += 1
            if num_added == BATCH_SIZE:
                output += self.data_base.execute_commands()
                num_added = 0
        output += self.data_base.execute_commands()
        decoded_outputs += [self.rdf_parser.decode_items(x) for x in output]
        end_time = time.time() * 1000
        return (end_time - start_time) / (len(all_keys))


    def evaluate_subject(self):
        all_keys = []
        for s, p, o in self.rdf_parser.graph:
            s = self.rdf_parser.encode_item(s.n3(self.rdf_parser.graph.namespace_manager))
            p = self.rdf_parser.encode_item(p.n3(self.rdf_parser.graph.namespace_manager))
            o = self.rdf_parser.encode_item(o.n3(self.rdf_parser.graph.namespace_manager))
            key = self.rdf_parser.build_key("subject", str(s))
            all_keys.append(key)
            key = self.rdf_parser.build_key("subject", str(o))
            all_keys.append(key)
        return self.evaluate_pattern_type(all_keys)

    def evaluate_predicate(self):
        all_keys = []
        for s, p, o in self.rdf_parser.graph:
            s = self.rdf_parser.encode_item(s.n3(self.rdf_parser.graph.namespace_manager))
            p = self.rdf_parser.encode_item(p.n3(self.rdf_parser.graph.namespace_manager))
            o = self.rdf_parser.encode_item(o.n3(self.rdf_parser.graph.namespace_manager))
            key = self.rdf_parser.build_key("predicate", str(p))
            all_keys.append(key)
            key = self.rdf_parser.build_key("predicate", str(o))
            all_keys.append(key)
        return self.evaluate_pattern_type(all_keys)

    def evaluate_object(self):
        all_keys = []
        for s, p, o in self.rdf_parser.graph:
            s = self.rdf_parser.encode_item(s.n3(self.rdf_parser.graph.namespace_manager))
            p = self.rdf_parser.encode_item(p.n3(self.rdf_parser.graph.namespace_manager))
            o = self.rdf_parser.encode_item(o.n3(self.rdf_parser.graph.namespace_manager))
            key = self.rdf_parser.build_key("object", str(o))
            all_keys.append(key)
            key = self.rdf_parser.build_key("object", str(s))
            all_keys.append(key)
        return self.evaluate_pattern_type(all_keys)

    def evaluate_subject_object(self):
        all_keys = []
        for s, p, o in self.rdf_parser.graph:
            s = self.rdf_parser.encode_item(s.n3(self.rdf_parser.graph.namespace_manager))
            p = self.rdf_parser.encode_item(p.n3(self.rdf_parser.graph.namespace_manager))
            o = self.rdf_parser.encode_item(o.n3(self.rdf_parser.graph.namespace_manager))
            key = self.rdf_parser.build_key("subject-object", self.rdf_parser.concatenate_items(s, o))
            all_keys.append(key)
            key = self.rdf_parser.build_key("subject-object", str(p))
            all_keys.append(key)
        return self.evaluate_pattern_type(all_keys)

    def evaluate_subject_predicate(self):
        all_keys = []
        for s, p, o in self.rdf_parser.graph:
            s = self.rdf_parser.encode_item(s.n3(self.rdf_parser.graph.namespace_manager))
            p = self.rdf_parser.encode_item(p.n3(self.rdf_parser.graph.namespace_manager))
            o = self.rdf_parser.encode_item(o.n3(self.rdf_parser.graph.namespace_manager))
            key = self.rdf_parser.build_key("subject-predicate", self.rdf_parser.concatenate_items(s, p))
            all_keys.append(key)
            key = self.rdf_parser.build_key("subject-predicate", str(o))
            all_keys.append(key)
        return self.evaluate_pattern_type(all_keys)

    def evaluate_predicate_object(self):
        all_keys = []
        for s, p, o in self.rdf_parser.graph:
            s = self.rdf_parser.encode_item(s.n3(self.rdf_parser.graph.namespace_manager))
            p = self.rdf_parser.encode_item(p.n3(self.rdf_parser.graph.namespace_manager))
            o = self.rdf_parser.encode_item(o.n3(self.rdf_parser.graph.namespace_manager))
            key = self.rdf_parser.build_key("predicate-object", self.rdf_parser.concatenate_items(p, o))
            all_keys.append(key)
            key = self.rdf_parser.build_key("predicate-object", str(s))
            all_keys.append(key)
        return self.evaluate_pattern_type(all_keys)

    def evaluate_subject_predicate_object(self):
        # can not be done with "bulk get", so the time will be too high
        pass

    def evaluate_all(self):
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
