from typing import Optional

from src import rdf_loader
from src.DataStorage import DataStorage
from src.rdf_loader import RDFLoader
import src.rdf_loader


class Utilities:

    def __init__(self, database: DataStorage, rdf_loader_obj: RDFLoader):
        self.database = database
        self.rdf_loader_obj = rdf_loader_obj

    def bulk_get_triple(self, filename):
        output = []
        triples = rdf_loader.read_file(filename, self.rdf_loader_obj, False)
        for s, p, o in triples:
            output += self.get_triple(s, p, o)

        return output

    def bulk_add_triple(self, filename):
        triples = rdf_loader.read_file(filename, self.rdf_loader_obj, False)
        for s, p, o in triples:
            self.add_triple(s, p, o)

    def bulk_delete_triple(self, filename):
        triples = rdf_loader.read_file(filename, self.rdf_loader_obj, False)
        for s, p, o in triples:
            self.delete_triple(s, p, o)

    def get_triple(self,
                   subject_pattern: Optional[str] = "",
                   predicate_pattern: Optional[str] = "",
                   object_pattern: Optional[str] = ""
                   ):
        if subject_pattern and (not object_pattern) and (not predicate_pattern):
            subject_pattern = self.rdf_loader_obj.encode_item(subject_pattern)
            item = f"{subject_pattern}"
            return self.rdf_loader_obj.decode_items(self.database.get_item("subject.db", item))

        if object_pattern and (not subject_pattern) and (not predicate_pattern):
            object_pattern = self.rdf_loader_obj.encode_item(object_pattern)
            item = f"{object_pattern}"
            return self.rdf_loader_obj.decode_items(self.database.get_item("object.db", item))

        if predicate_pattern and (not subject_pattern) and (not object_pattern):
            predicate_pattern = self.rdf_loader_obj.encode_item(predicate_pattern)
            item = f"{predicate_pattern}"
            return self.rdf_loader_obj.decode_items(self.database.get_item("predicate.db", item))

        if subject_pattern and object_pattern and (not predicate_pattern):
            subject_pattern = self.rdf_loader_obj.encode_item(subject_pattern)
            object_pattern = self.rdf_loader_obj.encode_item(object_pattern)
            item = f"{subject_pattern}-{object_pattern}"
            return self.rdf_loader_obj.decode_items(self.database.get_item("subject-object.db", item))

        if subject_pattern and predicate_pattern and (not object_pattern):
            subject_pattern = self.rdf_loader_obj.encode_item(subject_pattern)
            predicate_pattern = self.rdf_loader_obj.encode_item(predicate_pattern)
            item = f"{subject_pattern}-{predicate_pattern}"
            return self.rdf_loader_obj.decode_items(self.database.get_item("subject-predicate.db", item))

        if object_pattern and predicate_pattern and (not subject_pattern):
            object_pattern = self.rdf_loader_obj.encode_item(object_pattern)
            predicate_pattern = self.rdf_loader_obj.encode_item(predicate_pattern)
            item = f"{predicate_pattern}-{object_pattern}"
            return self.rdf_loader_obj.decode_items(self.database.get_item("predicate-object.db", item))

        if subject_pattern and predicate_pattern and object_pattern:
            subject_pattern = self.rdf_loader_obj.encode_item(subject_pattern)
            object_pattern = self.rdf_loader_obj.encode_item(object_pattern)
            predicate_pattern = self.rdf_loader_obj.encode_item(predicate_pattern)
            item = f"{subject_pattern}-{predicate_pattern}-{object_pattern}"

            if self.database.get_item("subject-predicate-object.db", item):
                return subject_pattern, predicate_pattern, object_pattern
            else:
                return None

    def delete_triple(self,
                      subject_pattern: str,
                      predicate_pattern: str,
                      object_pattern: str
                      ) -> None:
        if not self.get_triple(subject_pattern, predicate_pattern, object_pattern):
            return

        subject_pattern = self.rdf_loader_obj.encode_item(subject_pattern)
        object_pattern = self.rdf_loader_obj.encode_item(object_pattern)
        predicate_pattern = self.rdf_loader_obj.encode_item(predicate_pattern)
        self.database.delete_item("subject.db", f"{subject_pattern}", (predicate_pattern, object_pattern))
        self.database.delete_item("predicate.db", f"{predicate_pattern}", (subject_pattern, object_pattern))
        self.database.delete_item("object.db", f"{object_pattern}", (subject_pattern, predicate_pattern))
        self.database.delete_item("subject-predicate.db", f"{subject_pattern}-{predicate_pattern}", object_pattern)
        self.database.delete_item("subject-object.db", f"{subject_pattern}-{object_pattern}", predicate_pattern)
        self.database.delete_item("predicate-object.db", f"{predicate_pattern}-{object_pattern}", subject_pattern)
        self.database.delete_item("subject-predicate-object.db",
                                  f"{subject_pattern}-{predicate_pattern}-{object_pattern}", 1)

    def add_triple(self,
                   subject_pattern: str,
                   predicate_pattern: str,
                   object_pattern: str
                   ) -> None:
        if self.get_triple(subject_pattern, predicate_pattern, object_pattern):
            return
        subject_pattern = self.rdf_loader_obj.encode_item(subject_pattern)
        object_pattern = self.rdf_loader_obj.encode_item(object_pattern)
        predicate_pattern = self.rdf_loader_obj.encode_item(predicate_pattern)
        self.database.add_item("subject.db", f"{subject_pattern}", (predicate_pattern, object_pattern))
        self.database.add_item("predicate.db", f"{predicate_pattern}", (subject_pattern, object_pattern))
        self.database.add_item("object.db", f"{object_pattern}", (subject_pattern, predicate_pattern))
        self.database.add_item("subject-predicate.db", f"{subject_pattern}-{predicate_pattern}", object_pattern)
        self.database.add_item("subject-object.db", f"{subject_pattern}-{object_pattern}", predicate_pattern)
        self.database.add_item("predicate-object.db", f"{predicate_pattern}-{object_pattern}", subject_pattern)
        self.database.add_item("subject-predicate-object.db", f"{subject_pattern}-{predicate_pattern}-{object_pattern}",
                               1)
