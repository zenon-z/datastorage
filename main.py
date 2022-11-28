from typing import Optional

from DataStorage import DataStorage
from rdflib import Graph

from RDFParser import RDFParser
from sparql_utilities import find_pattern_value
from Evaluator import Evaluator
import re


# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def init_db():
    g = Graph()
    g.parse('https://dbpedia.org/ontology/data/definitions.ttl')
    g.serialize(format='turtle')
    db = DataStorage()
    for db_name in db.databases_names:
        for s, p, o in g.triples((None, None, None)):
            key = s
            value = f"p: {p} - o: {o}"
            db.insert(db_name, key, value)
        db.save_db(db_name)

    read_db(db, 'subject.db')


def read_db(db, db_name: str):
    for key in db.get_keys(db_name):
        print(f"s: {key}")
        print(db.get_item(db_name, key))
        print("\n")


def evaluate_all():
    ev = Evaluator(storage_model, graph_parser)
    ev.evaluate_triple(subject_pattern="dbo:فائل",
                       predicate_pattern="wdrs:describedby",
                       object_pattern="<http://dbpedia.org/ontology/data/definitions.ttl>")

    ev.evaluate_triple(subject_pattern="dbo:فائل",
                       predicate_pattern="",
                       object_pattern="<http://dbpedia.org/ontology/data/definitions.ttl>")

    ev.evaluate_triple(subject_pattern="dbo:فائل",
                       predicate_pattern="wdrs:describedby",
                       object_pattern="")

    ev.evaluate_triple(subject_pattern="dbo:فائل",
                       predicate_pattern="",
                       object_pattern="")

    ev.evaluate_triple(subject_pattern="",
                       predicate_pattern="wdrs:describedby",
                       object_pattern="<http://dbpedia.org/ontology/data/definitions.ttl>")

    ev.evaluate_triple(subject_pattern="",
                       predicate_pattern="",
                       object_pattern="<http://dbpedia.org/ontology/data/definitions.ttl>")

    ev.evaluate_triple(subject_pattern="",
                       predicate_pattern="wdrs:describedby",
                       object_pattern="")


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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    graph_url = 'https://dbpedia.org/ontology/data/definitions.ttl'
    storage_model = DataStorage()
    graph_parser = RDFParser(graph_url)
    graph_parser.fill_data(storage_model)

    # answer = find_pattern_value(storage_model, graph_parser, object_pattern="https://www.w3.org/People/Berners-Lee/")
    # print(answer)
    # convert_output()
    evaluate_all()

    # one_item = storage_model.get_item('object.db', "https://www.w3.org/People/Berners-Lee/")
    # print(one_item)
