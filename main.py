import time
from DataStorage import DataStorage
from RDFParser import RDFParser
from sparql_utilities import find_pattern_value2
from Evaluator import Evaluator
from Database import Database


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


def parse_pattern_variable(pattern_var):
    return "" if pattern_var[0] == "?" else pattern_var


if __name__ == '__main__':
    graph_url = 'https://dbpedia.org/ontology/data/definitions.ttl'
    storage_model = DataStorage()
    redis_db = Database("localhost", 6379, 0)
    start_time = time.time()
    graph_parser = RDFParser(graph_url)
    graph_parser.fill_data(redis_db)
    end_time = time.time()
    a = end_time - start_time
    print("loading time is ", a)
    evaluator = Evaluator(redis_db, graph_parser)
    evaluator.evaluate_all()
    # result = find_pattern_value2(storage_model=storage_model, rdf_parser=graph_parser, predicate_pattern="ov:describes")
    # print(result)
    storage_model.delete_all()

    while True:
        parse_command(input("Give a triple\n"))
