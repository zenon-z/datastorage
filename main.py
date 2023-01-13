import time

import rdf_loader
from Database import Database
from DataStorage import DataStorage
from rdf_loader import RDFLoader
from Evaluator import Evaluator
from RDFParser import RDFParser
from Utilities import Utilities

# def check_add_get_delete(utilities):
#     obj = utilities.get_triple(subject_pattern="http://dbpedia.org/ontology/americanComedyAward")
#     print(obj)
#     utilities.delete_triple("http://dbpedia.org/ontology/americanComedyAward",
#                             "http://www.w3.org/2007/05/powder-s#describedby",
#                             "http://dbpedia.org/ontology/data/definitions.ttl")
#     obj = utilities.get_triple(subject_pattern="http://dbpedia.org/ontology/americanComedyAward")
#     print(obj)
#     utilities.add_triple("http://dbpedia.org/ontology/americanComedyAward",
#                          "http://www.w3.org/2007/05/powder-s#describedby",
#                          "http://dbpedia.org/ontology/data/definitions.ttl")
#     obj = utilities.get_triple(subject_pattern="http://dbpedia.org/ontology/americanComedyAward")
#     print(obj)
"""
def parse_command(command):
    commands = ["ADD_TRIPLE", "QUERY_TRIPLE", "DELETE_TRIPLE", "BULK_ADD", "BULK_UPDATE", "BULK_DELETE"]
    parameters = command.split(" ")
    if len(parameters) != 4 or not parameters[0] in commands:
        print("UNKNOWN COMMAND")

    if parameters[0] == "ADD_TRIPLE":
        a = "a"
    if parameters[0] == "QUERY_TRIPLE":
        answer = find_pattern_value2(redis_db,
                                     graph_parser,
                                     subject_pattern=parse_pattern_variable(parameters[1]),
                                     predicate_pattern=parse_pattern_variable(parameters[2]),
                                     object_pattern=parse_pattern_variable(parameters[3]))
        print(answer)
"""

if __name__ == '__main__':
    graph_url2 = 'https://dbpedia.org/ontology/data/definitions.ttl'
    graph_url = "graphs/output.ttl"
    graph_name = "output"
    redis_db = Database("localhost", 6379, 0)
    db_storage = DataStorage()
    start_time = time.time()
    rdf_loader_obj = RDFLoader(graph_name)
    encoded_triples = rdf_loader.read_file(graph_url, rdf_loader_obj, db_storage)
    if db_storage.graph_exists(graph_name):
        print("Graph exists")
        rdf_loader_obj.load_all(redis_db)
    else:
        print("Graph dont exist")
        rdf_loader.process_data(encoded_triples, db_storage, graph_name)
        rdf_loader_obj.save_all(redis_db)

    end_time = time.time()
    a = end_time - start_time
    print("loading time is ", a)

    print("Saving to file now...")
    start_time = time.time()
    db_storage.save_all_db()
    end_time = time.time()
    a = end_time - start_time
    print("saving time is ", a)

    # graph_parser = RDFParser(graph_name, graph_url)
    # start_time = time.time()
    # if not redis_db.graph_exists(graph_name):
    #     graph_parser.fill_data(redis_db)
    # else:
    #     graph_parser.load_all(redis_db)
    # end_time = time.time()
    # a = end_time - start_time
    # print("loading time is ", a)
    print("Starting evaluation")
    evaluator = Evaluator(db_storage, encoded_triples, graph_name, rdf_loader_obj)
    evaluator.evaluate_all()
    # utilities = Utilities(redis_db, graph_parser)
    # utilities.add_triple('a', 'b', 'c')
    # utilities.add_triple('a', 'b', 'm')
    # utilities.add_triple('a', 'b', 'm')
    # print(utilities.get_triple(predicate_pattern='b'))
    # utilities.delete_triple('a', 'b', 'm')
    # print(utilities.get_triple(predicate_pattern='b'))
