import re
from typing import Optional, List, Tuple, Union

from DataStorage import DataStorage
from RDFParser import RDFParser


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
    #return everything


