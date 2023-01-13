from typing import List


def read_file(filename, rdf_loader, db):
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

            encoded_s = rdf_loader.encode_item(current_s)

            encoded_p = rdf_loader.encode_item(current_p)
            encoded_o = rdf_loader.encode_item(o)

            encoded_triples.append((encoded_s, encoded_p, encoded_o))
            previous_line_ending = line[-1]

    return encoded_triples


def process_data(encoded_triples, db, graph_name):
    num_added = 0
    num_batches = 0

    for encoded_s, encoded_p, encoded_o in encoded_triples:
        db.add_item("subject.db", build_key(encoded_s), (encoded_p, encoded_o))
        db.add_item("predicate.db", build_key(encoded_p), (encoded_s, encoded_o))
        db.add_item("object.db", build_key(encoded_o), (encoded_s, encoded_p))
        db.add_item("subject-predicate.db", build_key((encoded_s, encoded_p)), encoded_o)
        db.add_item("subject-object.db", build_key((encoded_s, encoded_o)), encoded_p)
        db.add_item("predicate-object.db", build_key((encoded_p, encoded_o)), encoded_s)
        db.add_item("subject-predicate-object.db", build_key((encoded_s, encoded_p, encoded_o)), 1)
        num_added += 1

        if num_added == 10000:
            num_added = 0
            num_batches += 1
            print(f"Batch sent: {num_batches * 10000}")


# def build_key(graph_name, table_name, key):
#     return str(key)


def build_key(key):
    if type(key) is tuple:
        key = "".join([str(x) for x in list(key)])
    return str(key)

    # encoded_key = b''
    # if type(key) is tuple:
    #     for x in key:
    #         byte_length = (x.bit_length() + 7) // 8
    #         x = x.to_bytes(byte_length, 'big')
    #         encoded_key += x
    # else:
    #     byte_length = (key.bit_length() + 7) // 8
    #     encoded_key = key.to_bytes(byte_length, 'big')
    #
    # return str(encoded_key)


class RDFLoader:

    def __init__(self, graph_name):
        self.graph_name = graph_name
        self.mapped_values = {}
        self.values_dict = {}

    def encode_item(self, value: str = None) -> str:
        try:
            encoded_value = self.values_dict[value]
        except KeyError:
            mapped_id = len(self.mapped_values)
            self.mapped_values[mapped_id] = value
            self.values_dict[value] = mapped_id
            encoded_value = mapped_id
        return encoded_value

    def decode_item(self, item: bytes) -> tuple:
        decoded = []
        try:
            for results in item:
                decoded.append(self.mapped_values[results])
        except TypeError:
            decoded.append(self.mapped_values[item])
        return tuple(decoded)

    def decode_items(self, items: List[str]):
        return [self.decode_item(item) for item in items]

    def save_all(self, db):
        db.set_dict("mapped_values", self.mapped_values)
        # db.set_dict(build_key(self.graph_name, "ids", "values_dict"), self.values_dict)

    def load_all(self, db):
        self.mapped_values = db.get_dict("mapped_values")
        # self.values_dict = db.get_dict(build_key(self.graph_name, "ids", "values_dict"))
