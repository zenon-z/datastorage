from typing import Optional, List, Union

from DataStorage import DataStorage
from RDFParser import RDFParser


class GraphUtilities:

    def __init__(self, storage_model: DataStorage, rdf_parser: RDFParser):
        self.storage_model = storage_model
        self.rdf_parser = rdf_parser

    def get_triple(self,
                   subject_pattern: Optional[str] = "",
                   predicate_pattern: Optional[str] = "",
                   object_pattern: Optional[str] = ""
                   ) -> Union[str, List[str]]:
        if subject_pattern and (not object_pattern) and (not predicate_pattern):
            #item = self.rdf_parser.encode_item(subject_pattern)
            #return self.storage_model.get_item("subject.db", item)
            item = self.rdf_parser.build_key("subject", str(subject_pattern))
            return self.storage_model.get_item(item)

        if object_pattern and (not subject_pattern) and (not predicate_pattern):
            item = self.rdf_parser.encode_item(object_pattern)
            return self.storage_model.get_item("object.db", item)

        if predicate_pattern and (not subject_pattern) and (not object_pattern):
            item = self.rdf_parser.encode_item(predicate_pattern)
            return self.storage_model.get_item("predicate.db", item)

        if subject_pattern and object_pattern and (not predicate_pattern):
            item1 = self.rdf_parser.encode_item(subject_pattern)
            item2 = self.rdf_parser.encode_item(object_pattern)
            item = self.rdf_parser.concatenate_items(item1, item2)
            return self.storage_model.get_item("subject-object.db", item)

        if subject_pattern and predicate_pattern and (not object_pattern):
            item1 = self.rdf_parser.encode_item(subject_pattern)
            item2 = self.rdf_parser.encode_item(predicate_pattern)
            item = self.rdf_parser.concatenate_items(item1, item2)
            return self.storage_model.get_item("subject-predicate.db", item)

        if object_pattern and predicate_pattern and (not subject_pattern):
            item1 = self.rdf_parser.encode_item(object_pattern)
            item2 = self.rdf_parser.encode_item(predicate_pattern)
            item = self.rdf_parser.concatenate_items(item1, item2)
            return self.storage_model.get_item("object-predicate.db", item)

        if subject_pattern and object_pattern and predicate_pattern:
            item1 = self.rdf_parser.encode_item(object_pattern)
            item2 = self.rdf_parser.encode_item(predicate_pattern)
            item = self.rdf_parser.concatenate_items(item1, item2)
            result = self.storage_model.get_item("object-predicate.db", item)
            if isinstance(result, str):
                results = result.split("\n")
                if subject_pattern in results:
                    return f"{subject_pattern} {predicate_pattern} {object_pattern}"
            return False

        else:
            self.rdf_parser.get_all_triples()

    def delete_triple(self,
                      subject_pattern: str,
                      predicate_pattern: str,
                      object_pattern: str
                      ) -> None:
        if not self.get_triple(subject_pattern, predicate_pattern, object_pattern):
            return
        encoded_subject = self.rdf_parser.encode_item(subject_pattern)
        self.storage_model.delete_item("subject.db", encoded_subject)
        encoded_object = self.rdf_parser.encode_item(object_pattern)
        self.storage_model.delete_item("object.db", encoded_object)
        encoded_predicate = self.rdf_parser.encode_item(predicate_pattern)
        self.storage_model.delete_item("predicate.db", encoded_predicate)
        encoded_subject_object = self.rdf_parser.concatenate_items(encoded_subject, encoded_object)
        self.storage_model.delete_item("subject-object.db", encoded_subject_object)
        encoded_subject_predicate = self.rdf_parser.concatenate_items(encoded_subject, encoded_predicate)
        self.storage_model.delete_item("subject-predicate.db", encoded_subject_predicate)
        encoded_object_predicate = self.rdf_parser.concatenate_items(encoded_object, encoded_predicate)
        self.storage_model.delete_item("object-predicate.db", encoded_object_predicate)
        self.storage_model.save_all_db()

    def add_triple(self,
                   subject_pattern: str,
                   predicate_pattern: str,
                   object_pattern: str
                   ) -> None:
        if self.get_triple(subject_pattern, predicate_pattern, object_pattern):
            return
        encoded_subject = self.rdf_parser.encode_item(subject_pattern)
        predicate_object = self.rdf_parser.concatenate_items(predicate_pattern, object_pattern)
        self.storage_model.insert_item("subject.db", encoded_subject, predicate_object)
        encoded_object = self.rdf_parser.encode_item(object_pattern)
        subject_predicate = self.rdf_parser.concatenate_items(subject_pattern, predicate_pattern)
        self.storage_model.insert_item("object.db", encoded_object, subject_predicate)
        encoded_predicate = self.rdf_parser.encode_item(predicate_pattern)
        object_subject = self.rdf_parser.concatenate_items(object_pattern, subject_pattern)
        self.storage_model.insert_item("predicate.db", encoded_predicate, object_subject)
        encoded_subject_object = self.rdf_parser.concatenate_items(encoded_subject, encoded_object)
        self.storage_model.insert_item("subject-object.db", encoded_subject_object, predicate_pattern)
        encoded_subject_predicate = self.rdf_parser.concatenate_items(encoded_subject, encoded_predicate)
        self.storage_model.insert_item("subject-predicate.db", encoded_subject_predicate, object_pattern)
        encoded_object_predicate = self.rdf_parser.concatenate_items(encoded_object, encoded_predicate)
        self.storage_model.insert_item("object-predicate.db", encoded_object_predicate, subject_pattern)
        self.storage_model.save_all_db()
