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
        start_time = time.time()
        result = sparql_util.find_pattern_value(self.storage_model, self.rdf_parser, subject_pattern, predicate_pattern,
                                                object_pattern)
        end_time = time.time()
        print(result, (end_time - start_time) * 1000)

    def evaluate_all(self, predicate_pattern="wdrs:describedby"):
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
