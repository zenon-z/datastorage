import time

import rdf_loader
from src.DataStorage import DataStorage
from rdf_loader import RDFLoader
from src.Evaluator import Evaluator
from Utilities import Utilities

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
    graph_url = "graphs/dbpedia.ttl"
    graph_name = "dbpedia"

    db_storage = DataStorage()
    start_time = time.time()
    rdf_loader_obj = RDFLoader(graph_name)
    rdf_loader_obj.encoded_triples = rdf_loader.read_file(graph_url, rdf_loader_obj)
    file_read_time = time.time()

    print(f"File reading took: {file_read_time - start_time}")
    if db_storage.graph_exists(graph_name):
        print("Graph exists")
        rdf_loader_obj.values_dict = db_storage.get_item('values-dict.db', graph_name)[0]
        rdf_loader_obj.mapped_values = db_storage.get_item('mapped-values.db', graph_name)[0]
        rdf_loader_obj.encoded_triples = db_storage.get_item("triples.db", graph_name)[0]
    else:
        print("Graph dont exist")
        batch_times = rdf_loader.process_data(rdf_loader_obj.encoded_triples, db_storage)
        with open('databases/batch_times.txt', 'w') as fp:
            for batch_time in batch_times:
                fp.write(f"{batch_time}\n")

        end_time = time.time()
        a = end_time - start_time
        print("loading time is ", a)

        print("Saving to file now...")
        start_time = time.time()

        rdf_loader_obj.add_mappings_dictionaries(db_storage)
        db_storage.save_all_db()

        end_time = time.time()
        a = end_time - start_time
        print("saving time is ", a)

    evaluator = Evaluator(db_storage, rdf_loader_obj.encoded_triples, graph_name, rdf_loader_obj)
    evaluator.evaluate_all()
    utilities = Utilities(db_storage, rdf_loader_obj)
    utilities.add_triple('a', 'b', 'c')
    utilities.add_triple('a', 'b', 'm')
    utilities.add_triple('a', 'b', 'm')
    print(utilities.get_triple(predicate_pattern='b'))
    utilities.delete_triple('a', 'b', 'm')
    print(utilities.get_triple(predicate_pattern='b'))
