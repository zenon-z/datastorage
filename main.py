from DataStorage import DataStorage
from rdflib import Graph

from RDFParser import RDFParser


# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def init_db():
    g = Graph()
    g.parse("http://www.w3.org/People/Berners-Lee/card")
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    graph_url = "http://www.w3.org/People/Berners-Lee/card"
    storage_model = DataStorage()
    graph_parser = RDFParser(graph_url)
    graph_parser.fill_data(storage_model)
    answer = find_pattern_value(storage_model, graph_parser, object_pattern="https://www.w3.org/People/Berners-Lee/")
    print(answer)
    # one_item = storage_model.get_item('object.db', "https://www.w3.org/People/Berners-Lee/")
    # print(one_item)
