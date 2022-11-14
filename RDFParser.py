from rdflib import Graph


class RDFParser:

    def __init__(self, graph_url: str) -> None:
        self.graph = Graph()
        self.graph.parse(graph_url)

    def parse_graph(self, graph_url: str):
        self.graph.parse(graph_url)

    def fill_data(self, db):
        for s, p, o in self.graph.triples((None, None, None)):
            db.insert('subject.db', s, f"p: {p} - o: {o}")
            db.insert('predicate.db', p, f"o: {o} - s: {s}")
            db.insert('object.db', o, f"s: {s} - p: {p}")
            db.insert('subject-predicate.db', f"s: {s} - p: {p}", o)
            db.insert('predicate-object.db', f"p: {p} - o: {o}", s)
            db.insert('subject-object.db', f"s: {s} - o: {o}", p)
        db.save_all()
