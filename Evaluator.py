from typing import Optional
import time
from DataStorage import DataStorage
from RDFParser import RDFParser
import sparql_utilities as sparql_util


class Evaluator:

    def __init__(self, storage_model: DataStorage, rdf_parser: RDFParser):
        self.storage_model = storage_model
        self.rdf_parser = rdf_parser

    def evaluate_triple(self,
                        subject_pattern: Optional[str] = "",
                        predicate_pattern: Optional[str] = "",
                        object_pattern: Optional[str] = ""):
        """
        Time to evaluate one pattern.
        :param subject_pattern:
        :param predicate_pattern:
        :param object_pattern:
        :return:
        """
        start_time = time.time() * 1000
        result = sparql_util.find_pattern_value(self.storage_model, self.rdf_parser, subject_pattern, predicate_pattern,
                                                object_pattern)
        end_time = time.time() * 1000
        return result, (end_time - start_time)
    def evaluate_subject_predicate_object(self):
        total_time_ms = 0
        false_time = 0
        for s, p, o in self.rdf_parser.graph:
            s_prefix = s.n3(self.rdf_parser.graph.namespace_manager)
            p_prefix = p.n3(self.rdf_parser.graph.namespace_manager)
            o_prefix = o.n3(self.rdf_parser.graph.namespace_manager)
            result, time = self.evaluate_triple(subject_pattern=s_prefix, predicate_pattern=p_prefix, object_pattern=o_prefix)
            result_2, time2 = self.evaluate_triple(object_pattern=o_prefix, predicate_pattern=s_prefix, subject_pattern=p_prefix)
            total_time_ms += time
            false_time += time2
            #print(f"for the predicate {p_prefix} and object {o_prefix}")
            #print(f"there is {len(result)} matching patterns")
            #print(f"and the time needed to get the answer is {time} ms")
        return (total_time_ms + false_time ) /(2*len(self.rdf_parser.graph))

    def evaluate_predicate_object(self):
        total_time_ms = 0
        false_time = 0
        for s, p, o in self.rdf_parser.graph:
            s_prefix = s.n3(self.rdf_parser.graph.namespace_manager)
            p_prefix = p.n3(self.rdf_parser.graph.namespace_manager)
            o_prefix = o.n3(self.rdf_parser.graph.namespace_manager)
            result, time = self.evaluate_triple(predicate_pattern=p_prefix, object_pattern=o_prefix)
            result_2, time2 = self.evaluate_triple(object_pattern=o_prefix, predicate_pattern=s_prefix)
            total_time_ms += time
            false_time += time2
            #print(f"for the predicate {p_prefix} and object {o_prefix}")
            #print(f"there is {len(result)} matching patterns")
            #print(f"and the time needed to get the answer is {time} ms")
        return (total_time_ms + false_time ) /(2*len(self.rdf_parser.graph))


    def evaluate_predicate_subject(self):
        total_time_ms = 0
        false_time = 0
        for s, p, o in self.rdf_parser.graph:
            s_prefix = s.n3(self.rdf_parser.graph.namespace_manager)
            p_prefix = p.n3(self.rdf_parser.graph.namespace_manager)
            o_prefix = o.n3(self.rdf_parser.graph.namespace_manager)
            result, time = self.evaluate_triple(predicate_pattern=p_prefix, subject_pattern=s_prefix)
            result_2, time2 = self.evaluate_triple(subject_pattern=o_prefix, predicate_pattern=s_prefix)
            total_time_ms += time
            false_time += time2
            #print(f"for the predicate {p_prefix} and object {o_prefix}")
            #print(f"there is {len(result)} matching patterns")
            #print(f"and the time needed to get the answer is {time} ms")
        return (total_time_ms + false_time ) /(2*len(self.rdf_parser.graph))


    def evaluate_object_subject(self):
        total_time_ms = 0
        false_time = 0
        for s, p, o in self.rdf_parser.graph:
            s_prefix = s.n3(self.rdf_parser.graph.namespace_manager)
            p_prefix = p.n3(self.rdf_parser.graph.namespace_manager)
            o_prefix = o.n3(self.rdf_parser.graph.namespace_manager)
            result, time = self.evaluate_triple(object_pattern=o_prefix, subject_pattern=s_prefix)
            result_2, time2 = self.evaluate_triple(subject_pattern=o_prefix, object_pattern=s_prefix)
            total_time_ms += time
            false_time += time2
            #print(f"for the predicate {p_prefix} and object {o_prefix}")
            #print(f"there is {len(result)} matching patterns")
            #print(f"and the time needed to get the answer is {time} ms")
        return (total_time_ms + false_time ) /(2*len(self.rdf_parser.graph))


    def evaluate_predicate(self):
        total_time_ms = 0
        false_time = 0
        for s, p, o in self.rdf_parser.graph:
            s_prefix = s.n3(self.rdf_parser.graph.namespace_manager)
            p_prefix = p.n3(self.rdf_parser.graph.namespace_manager)
            o_prefix = o.n3(self.rdf_parser.graph.namespace_manager)
            result, time = self.evaluate_triple(predicate_pattern=p_prefix)
            result2, time2 = self.evaluate_triple(predicate_pattern=s_prefix)
            total_time_ms += time
            false_time += time2
            #print(f"for the predicate predicate {p_prefix}")
            #print(f"there is {len(result)} matching patterns")
            #print(f"and the time needed to get the answer is {time} ms")
        return (total_time_ms + false_time ) /(2*len(self.rdf_parser.graph))


    def evaluate_subject(self):
        total_time_ms = 0
        false_time = 0
        for s, p, o in self.rdf_parser.graph:
            s_prefix = s.n3(self.rdf_parser.graph.namespace_manager)
            p_prefix = p.n3(self.rdf_parser.graph.namespace_manager)
            o_prefix = o.n3(self.rdf_parser.graph.namespace_manager)
            result, time = self.evaluate_triple(subject_pattern=s_prefix)
            result2, time2 = self.evaluate_triple(subject_pattern=o_prefix)
            total_time_ms += time
            false_time += time2
            #print(f"for the predicate predicate {p_prefix}")
            #print(f"there is {len(result)} matching patterns")
            #print(f"and the time needed to get the answer is {time} ms")
        return (total_time_ms + false_time ) /(2*len(self.rdf_parser.graph))


    def evaluate_object(self):
        total_time_ms = 0
        false_time = 0
        for s, p, o in self.rdf_parser.graph:
            s_prefix = s.n3(self.rdf_parser.graph.namespace_manager)
            p_prefix = p.n3(self.rdf_parser.graph.namespace_manager)
            o_prefix = o.n3(self.rdf_parser.graph.namespace_manager)
            result, time = self.evaluate_triple(object_pattern=o_prefix)
            result2, time2 = self.evaluate_triple(object_pattern=s_prefix)
            total_time_ms += time
            false_time += time2
            #print(f"for the predicate predicate {p_prefix}")
            #print(f"there is {len(result)} matching patterns")
            #print(f"and the time needed to get the answer is {time} ms")
        return (total_time_ms + false_time ) /(2*len(self.rdf_parser.graph))

    def evaluate_all(self):
        res0 = self.evaluate_subject_predicate_object()
        print(f"The time needed to search on a pattern that has everything is {res0} ms")
        res1 = self.evaluate_predicate_object()
        print(f"The time needed to search on a pattern that only the subject is known is {res1} ms")
        res2 = self.evaluate_object_subject()
        print(f"The time needed to search on a pattern that only the predicate is known is {res2} ms")
        res3 = self.evaluate_predicate_subject()
        print(
            f"The time needed to search on a pattern that only the object is known is {res3} ms")
        res4 = self.evaluate_predicate()
        print(
            f"The time needed to search on a pattern that its subject and object are known is {res4} ms")
        res5 = self.evaluate_subject()
        print(
            f"The time needed to search on a pattern that its predicate and object are known is {res5} ms")
        res6 = self.evaluate_object()
        print(
            f"The time needed to search on a pattern that its subject and predicate are known is {res6} ms")

        print(f"The average time to search on a triple is {(res0 + res1 + res2 + res3 + res4 + res5 + res6) / 7} ms, the graph has {len(self.rdf_parser.graph)} triples")


    def evaluate_all2(self, predicate_pattern="wdrs:describedby"):
        self.evaluate_triple(subject_pattern=predicate_pattern,
                             predicate_pattern=predicate_pattern,
                             object_pattern=predicate_pattern)

        self.evaluate_triple(subject_pattern=predicate_pattern,
                             predicate_pattern="",
                             object_pattern=predicate_pattern)

        self.evaluate_triple(subject_pattern=predicate_pattern,
                             predicate_pattern=predicate_pattern,
                             object_pattern="")

        self.evaluate_triple(subject_pattern="",
                             predicate_pattern=predicate_pattern,
                             object_pattern=predicate_pattern)

        self.evaluate_triple(subject_pattern=predicate_pattern,
                             predicate_pattern="",
                             object_pattern="")

        self.evaluate_triple(subject_pattern="",
                             predicate_pattern="",
                             object_pattern=predicate_pattern)

        self.evaluate_triple(subject_pattern="",
                             predicate_pattern=predicate_pattern,
                             object_pattern="")
        self.evaluate_triple(subject_pattern=predicate_pattern,
                             predicate_pattern="",
                             object_pattern="")
