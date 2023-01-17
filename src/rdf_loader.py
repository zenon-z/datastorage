from typing import List
import time


def read_file(filename, rdf_loader, encoded=True):
    f = open(filename, "r")
    encoded_triples = []
    current_s = ""
    current_p = ""
    previous_line_ending = "."

    for line in f:
        if line[0] == "@" or line[0] == '\n':
            continue
        else:

            # split up triples
            line = line.strip()
            # Whole new triple
            if previous_line_ending == ".":
                triple = line.split(" ")
                current_s = triple[0]
                current_p = triple[1]
                o = triple[2]
            # Only update predicate and object
            elif previous_line_ending == ";":
                triple = line.split(" ")
                current_p = triple[0]
                o = triple[1][0:-2]
            # Only update object
            elif previous_line_ending == ",":
                o = line[0:-2]

            if encoded:
                encoded_s = rdf_loader.encode_item(current_s)
                encoded_p = rdf_loader.encode_item(current_p)
                encoded_o = rdf_loader.encode_item(o)
                encoded_triples.append((encoded_s, encoded_p, encoded_o))
            else:
                encoded_triples.append((current_s, current_p, o))

            previous_line_ending = line[-1]

    return encoded_triples


def process_data(encoded_triples, db):
    num_added = 0
    num_batches = 0
    batch_times = []
    for encoded_s, encoded_p, encoded_o in encoded_triples:
        if num_added == 0:
            start_time = time.time()

        db.add_item("subject.db", f"{encoded_s}", (encoded_p, encoded_o))
        db.add_item("predicate.db", f"{encoded_p}", (encoded_s, encoded_o))
        db.add_item("object.db", f"{encoded_o}", (encoded_s, encoded_p))
        db.add_item("subject-predicate.db", f"{encoded_s}-{encoded_p}", encoded_o)
        db.add_item("subject-object.db", f"{encoded_s}-{encoded_o}", encoded_p)
        db.add_item("predicate-object.db", f"{encoded_p}-{encoded_o}", encoded_s)
        db.add_item("subject-predicate-object.db", f"{encoded_s}-{encoded_p}-{encoded_o}", 1)
        num_added += 1

        if num_added == 10000:
            num_added = 0
            num_batches += 1
            print(f"Batch sent: {num_batches * 10000}")
            end_time = time.time()
            batch_times.append(end_time - start_time)

    return batch_times


class RDFLoader:

    def __init__(self, graph_name):
        self.graph_name = graph_name
        self.mapped_values = {}
        self.values_dict = {}
        self.encoded_triples = []

    def encode_item(self, value: str = None) -> int:
        try:
            encoded_value = self.values_dict[value]
        except KeyError:
            mapped_id = len(self.mapped_values)
            self.mapped_values[mapped_id] = value
            self.values_dict[value] = mapped_id
            encoded_value = mapped_id
        return encoded_value

    def decode_item(self, item: tuple) -> tuple:
        decoded = []
        try:
            for results in item:
                decoded.append(self.mapped_values[results])
        except TypeError:
            decoded.append(self.mapped_values[item])
        return tuple(decoded)

    def decode_items(self, items: List[tuple]):
        return [self.decode_item(item) for item in items]

    def add_mappings_dictionaries(self, db):
        db.add_item("mapped-values.db", self.graph_name, self.mapped_values)
        db.add_item("values-dict.db", self.graph_name, self.values_dict)
        db.add_item("triples.db", self.graph_name, self.encoded_triples)
