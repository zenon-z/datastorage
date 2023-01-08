from typing import Optional, List, Union

from Database import Database
from RDFParser import RDFParser


class Utilities:

    def __init__(self, database: Database, rdf_parser: RDFParser):
        self.database = database
        self.rdf_parser = rdf_parser

    def bulk_get_triple(self, triples):
        output = []
        for s, p, o in triples:
            output += self.get_triple(s, p, o)

        return output

    def bulk_add_triple(self, triples):
        for s, p, o in triples:
            self.add_triple(s, p, o)

    def bulk_delete_triple(self, triples):
        for s, p, o in triples:
            self.delete_triple(s, p, o)

    def get_triple(self,
                   subject_pattern: Optional[str] = "",
                   predicate_pattern: Optional[str] = "",
                   object_pattern: Optional[str] = ""
                   ) -> Union[str, List[str]]:
        if subject_pattern and (not object_pattern) and (not predicate_pattern):
            subject_pattern = self.rdf_parser.encode_item(subject_pattern)
            item = self.rdf_parser.build_key("subject", str(subject_pattern))
            return self.rdf_parser.decode_items(self.database.get_item(item))

        if object_pattern and (not subject_pattern) and (not predicate_pattern):
            object_pattern = self.rdf_parser.encode_item(object_pattern)
            item = self.rdf_parser.build_key("object", str(object_pattern))
            return self.rdf_parser.decode_items(self.database.get_item(item))

        if predicate_pattern and (not subject_pattern) and (not object_pattern):
            predicate_pattern = self.rdf_parser.encode_item(predicate_pattern)
            item = self.rdf_parser.build_key("predicate", str(predicate_pattern))
            return self.rdf_parser.decode_items(self.database.get_item(item))

        if subject_pattern and object_pattern and (not predicate_pattern):
            subject_pattern = self.rdf_parser.encode_item(subject_pattern)
            object_pattern = self.rdf_parser.encode_item(object_pattern)
            item = self.rdf_parser.build_key("subject-object",
                                             self.rdf_parser.concatenate_items(subject_pattern, object_pattern))
            return self.rdf_parser.decode_items(self.database.get_item(item))

        if subject_pattern and predicate_pattern and (not object_pattern):
            subject_pattern = self.rdf_parser.encode_item(subject_pattern)
            predicate_pattern = self.rdf_parser.encode_item(predicate_pattern)
            item = self.rdf_parser.build_key("subject-predicate",
                                             self.rdf_parser.concatenate_items(subject_pattern, predicate_pattern))
            return self.rdf_parser.decode_items(self.database.get_item(item))

        if object_pattern and predicate_pattern and (not subject_pattern):
            object_pattern = self.rdf_parser.encode_item(object_pattern)
            predicate_pattern = self.rdf_parser.encode_item(predicate_pattern)
            item = self.rdf_parser.build_key("predicate-object",
                                             self.rdf_parser.concatenate_items(predicate_pattern, object_pattern))
            return self.rdf_parser.decode_items(self.database.get_item(item))

        if subject_pattern and predicate_pattern and object_pattern:
            subject_pattern = self.rdf_parser.encode_item(subject_pattern)
            object_pattern = self.rdf_parser.encode_item(object_pattern)
            predicate_pattern = self.rdf_parser.encode_item(predicate_pattern)
            item = self.rdf_parser.build_key("subject-predicate-object",
                                             self.rdf_parser.concatenate_items(subject_pattern,
                                                                               self.rdf_parser.concatenate_items(
                                                                                   predicate_pattern, object_pattern)))
            if self.database.get_item(item):
                return f"{subject_pattern} - {predicate_pattern} - {object_pattern}"
            else:
                return None

    def delete_triple(self,
                      subject_pattern: str,
                      predicate_pattern: str,
                      object_pattern: str
                      ) -> None:
        if not self.get_triple(subject_pattern, predicate_pattern, object_pattern):
            return

        subject_pattern = self.rdf_parser.encode_item(subject_pattern)
        object_pattern = self.rdf_parser.encode_item(object_pattern)
        predicate_pattern = self.rdf_parser.encode_item(predicate_pattern)
        self.database.delete_item(self.rdf_parser.build_key("subject", subject_pattern),
                                  self.rdf_parser.concatenate_items(predicate_pattern, object_pattern))
        self.database.delete_item(self.rdf_parser.build_key("predicate", predicate_pattern),
                                  self.rdf_parser.concatenate_items(subject_pattern, object_pattern))
        self.database.delete_item(self.rdf_parser.build_key("object", object_pattern),
                                  self.rdf_parser.concatenate_items(subject_pattern, predicate_pattern))

        self.database.delete_item(self.rdf_parser.build_key("subject-predicate",
                                                            self.rdf_parser.concatenate_items(subject_pattern,
                                                                                              predicate_pattern)),
                                  object_pattern)
        self.database.delete_item(self.rdf_parser.build_key("subject-object",
                                                            self.rdf_parser.concatenate_items(subject_pattern,
                                                                                              object_pattern)),
                                  predicate_pattern)
        self.database.delete_item(self.rdf_parser.build_key("predicate-object",
                                                            self.rdf_parser.concatenate_items(predicate_pattern,
                                                                                              object_pattern)),
                                  subject_pattern)

        self.database.delete_item(self.rdf_parser.build_key("subject-predicate-object",
                                                            self.rdf_parser.concatenate_items(subject_pattern,
                                                                                              self.rdf_parser.concatenate_items(
                                                                                                  predicate_pattern,
                                                                                                  object_pattern))),
                                  1)

    def add_triple(self,
                   subject_pattern: str,
                   predicate_pattern: str,
                   object_pattern: str
                   ) -> None:
        if self.get_triple(subject_pattern, predicate_pattern, object_pattern):
            return
        subject_pattern = self.rdf_parser.encode_item(subject_pattern)
        object_pattern = self.rdf_parser.encode_item(object_pattern)
        predicate_pattern = self.rdf_parser.encode_item(predicate_pattern)
        self.database.start_pipeline()
        self.database.add_item(self.rdf_parser.build_key("subject", subject_pattern),
                               self.rdf_parser.concatenate_items(predicate_pattern, object_pattern))
        self.database.add_item(self.rdf_parser.build_key("predicate", predicate_pattern),
                               self.rdf_parser.concatenate_items(subject_pattern, object_pattern))
        self.database.add_item(self.rdf_parser.build_key("object", object_pattern),
                               self.rdf_parser.concatenate_items(subject_pattern, predicate_pattern))
        self.database.add_item(self.rdf_parser.build_key("subject-predicate",
                                                         self.rdf_parser.concatenate_items(subject_pattern,
                                                                                           predicate_pattern)),
                               object_pattern)
        self.database.add_item(self.rdf_parser.build_key("subject-object",
                                                         self.rdf_parser.concatenate_items(subject_pattern,
                                                                                           object_pattern)),
                               predicate_pattern)
        self.database.add_item(self.rdf_parser.build_key("predicate-object",
                                                         self.rdf_parser.concatenate_items(predicate_pattern,
                                                                                           object_pattern)),
                               subject_pattern)
        self.database.add_item(self.rdf_parser.build_key("subject-predicate-object",
                                                         self.rdf_parser.concatenate_items(subject_pattern,
                                                                                           self.rdf_parser.concatenate_items(
                                                                                               predicate_pattern,
                                                                                               object_pattern))),
                               1)

        self.database.execute_commands()
