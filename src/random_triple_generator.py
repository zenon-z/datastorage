import random
import rdf_loader

names = ["s", "p", "o", "s-p", "s-o", "p-o", "s-p-o"]
replace = False


def initialize_triples_dict():
    triples = {}
    for name in names:
        triples[name] = []
    return triples


def load_prefixes(filename):
    prefixes = {}
    f = open(filename, "r")
    for line in f:
        if line[0] == "@":
            line = line.strip()
            names = line.split(" ")
            prefix = names[1][:-1]
            uri = clean_uri(names[2])
            prefixes[prefix] = uri
        else:
            break
    f.close()

    return prefixes


def replace_prefix(prefixes, value):

    if not replace:
        return value

    items = value.split(":")
    if items[0] in prefixes.keys():
        items[1] = clean_uri(items[1])
        return f"{prefixes[items[0]]}/{items[1]}"

    return clean_uri(value)


def clean_uri(uri):
    return uri.replace(">", "").replace("<", "")


def get_random_triples(filename):
    prefixes = load_prefixes(filename)
    f = open(filename, "r")
    num_added = 0
    triples = initialize_triples_dict()
    previous_line_ending = ""
    for line in f:
        if line[0] == "@" or line[0] == '\n':
            continue
        else:
            line = line.strip()
            # Whole new triple
            if previous_line_ending == ".":
                triple = line.split(" ")
                current_s = replace_prefix(prefixes, triple[0])
                current_p = replace_prefix(prefixes, triple[1])
                o = replace_prefix(prefixes, triple[2])
                if random.randint(0, 1000) == 1:
                    triples["s"].append(create_triple_pattern(s=current_s))
                    triples["p"].append(create_triple_pattern(p=current_p))
                    triples["o"].append(create_triple_pattern(o=o))
                    triples["s-p"].append(create_triple_pattern(s=current_s, p=current_p))
                    triples["s-o"].append(create_triple_pattern(s=current_s, o=o))
                    triples["p-o"].append(create_triple_pattern(p=current_p, o=o))
                    triples["s-p-o"].append(create_triple_pattern(s=current_s, p=current_p, o=o))
                    num_added += 1
            previous_line_ending = line[-1]

            if num_added == 50:
                break
    f.close()
    return triples


def create_triple_pattern(s="?s", p="?s", o="?s"):
    return f"{s} {p} {o} .\n"


def create_random_triples(filename):
    triples = get_random_triples(filename)

    for name in names:
        with open(rf'triples/triples_our/{name}_triples_dbpedia.txt', 'w') as fp:
            for triple in triples[name]:
                fp.write(f"{triple}")
            print(f"Saved triples/triples_our/{name}_triples_dbpedia.txt successfully")
