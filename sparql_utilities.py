from typing import Optional, List, Tuple, Union

from DataStorage import DataStorage
from RDFParser import RDFParser


def read_sparql_from_file(filename: str) -> list():
    # return list of sparql triple patterns
    pass


# def find_pattern_value(storage_model: DataStorage,
#                        rdf_parser: RDFParser,
#                        subject_pattern: Optional[str] = "",
#                        predicate_pattern: Optional[str] = "",
#                        object_pattern: Optional[str] = "") -> List[Tuple[str, str, str]]:
#     if subject_pattern:
#         if object_pattern:
#             storage_model.get_item("subject-object.db", map_item(f"s: {subject_pattern} - o: {object_pattern}"))
#             return query("subject-object.db")
#         if predicate_pattern:
#             return query("subject-predicate.db")
#         return query("subject.db")
#     if predicate_pattern:
#         if object_pattern:
#             return query("predicate-object.db")
#         return query("predicate.db")
#     if object_pattern:
#         return query("object.db")

def find_pattern_value(storage_model: DataStorage,
                       rdf_parser: RDFParser,
                       subject_pattern: Optional[str] = "",
                       predicate_pattern: Optional[str] = "",
                       object_pattern: Optional[str] = "") -> Union[str, List[str]]:
    if subject_pattern and (not object_pattern) and (not predicate_pattern):
        item = rdf_parser.map_item(subject_pattern)
        return storage_model.get_item("subject.db", item)
    if object_pattern and (not subject_pattern) and (not predicate_pattern):
        item = rdf_parser.map_item(object_pattern)
        return storage_model.get_item("object.db", item)
    if predicate_pattern and (not subject_pattern) and (not object_pattern):
        item = rdf_parser.map_item(predicate_pattern)
        return storage_model.get_item("predicate.db", item)
    if subject_pattern and object_pattern and (not predicate_pattern):
        item = rdf_parser.map_item(subject_pattern, object_pattern)
        return storage_model.get_item("subject-object.db", item)
    if subject_pattern and predicate_pattern and (not object_pattern):
        item = rdf_parser.map_item(subject_pattern, predicate_pattern)
        return storage_model.get_item("subject-predicate.db", item)
    if object_pattern and predicate_pattern and (not subject_pattern):
        item = rdf_parser.map_item(object_pattern, predicate_pattern)
        return storage_model.get_item("predicate-object.db", item)
    # return everything
