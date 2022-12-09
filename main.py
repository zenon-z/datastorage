import re
import time
from typing import Optional

from DataStorage import DataStorage
from rdflib import Graph

from GraphStorage import GraphStorage
from RDFParser import RDFParser
from sparql_utilities import find_pattern_value
from Evaluator import Evaluator


def one_url():
    start_time = time.time()
    graph_url = 'https://dbpedia.org/ontology/data/definitions.ttl'
    storage_model = DataStorage()
    graph_parser = RDFParser(graph_url)
    graph_parser.fill_data(storage_model)
    end_time = time.time()
    print("loading time is", end_time - start_time)

    evaluator = Evaluator(storage_model, graph_parser)
    evaluator.evaluate_all()
    storage_model.delete_all()
"""
def convert_output():
    answer = find_pattern_value(storage_model, graph_parser, predicate_pattern="wdrs:describedby")
    index = 0
    for solution in answer.split("\n"):
        print(f"OMEGA{index}= {{")
        for response in solution.split("-"):
            rep = re.search("([spo]): ", response)
            if rep is not None:
                position = rep.group(0)
                print(f"{get_variable_name(position)}={response.split(position)[1].strip()}")
        print("}")
        index = index + 1


def get_variable_name(position):
    return f"?{position[0]}"
"""

if __name__ == '__main__':
    graph_url = 'https://dbpedia.org/ontology/data/definitions.ttl'
    start_time = time.time()
    storage_model = DataStorage()
    graph_parser = RDFParser(graph_url)
    graph_parser.fill_data(storage_model)
    end_time = time.time()
    a = end_time - start_time
    print("loading time is ", a)
    evaluator = Evaluator(storage_model, graph_parser)
    evaluator.evaluate_all()
    result = find_pattern_value(storage_model=storage_model, rdf_parser=graph_parser, predicate_pattern="ov:describes")
    print(result)
    storage_model.delete_all()
