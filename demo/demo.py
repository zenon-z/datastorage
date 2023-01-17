from src import rdf_loader
from ModelCli import ModelCli
from src.rdf_loader import RDFLoader
from src.Utilities import Utilities
from src.DataStorage import DataStorage


if __name__ == '__main__':
    db_storage = DataStorage()
    graph_dir = input("Enter the turtle file\n")
    graph_name = input("Enter the desired graph name\n")

    rdf_loader_obj = RDFLoader(graph_name)
    rdf_loader_obj.encoded_triples = rdf_loader.read_file(graph_dir, rdf_loader_obj)
    rdf_loader.process_data(rdf_loader_obj.encoded_triples, db_storage)
    utilities = Utilities(db_storage, rdf_loader_obj)
    model_cli = ModelCli(utilities)
    model_cli.start_cli()




