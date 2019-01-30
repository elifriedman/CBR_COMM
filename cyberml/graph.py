import networkx as nx
import igraph

class Graph(object):
    def __init__(self):
        self.edges = set()
        self.nodes = set()
        self._name2id = {}
        self._networkx = None
        self._igraph = None

    @classmethod
    def load_from_csv(cls, filename):
        g = cls()
        id = 0
        with open(filename) as f:
            for line in f:
                edgeinfo = [v.strip() for v in line.split(',')]
                node1, node2 = edgeinfo[0], edgeinfo[1]
                w = edgeinfo[2] if len(edgeinfo) >= 3 else 1
                if node1 not in g._name2id:
                    g._name2id[node1] = id
                    id += 1
                if node2 not in g._name2id:
                    g._name2id[node2] = id
                    id += 1
                node1 = g._name2id[node1]  # igraph requires ids rather than names
                node2 = g._name2id[node2]
                g.nodes.add(node1)
                g.nodes.add(node2)
                g.edges.add((node1, node2, w))
        return g

    def get_networkx(self):
        if self._networkx is None:
            self._networkx = nx.Graph()
            for v1, v2, w in self.edges:
                self._networkx.add_edge(v1, v2, w=w)
        return self._networkx

    def get_igraph(self):
        if self._igraph is None:
            self._igraph = igraph.Graph()
            self._igraph.add_vertices(range(len(self._name2id)))
            self._igraph.add_edges([(v1, v2) for v1, v2, w in self.edges])
            for v1, v2, w in self.edges:
                self._igraph[v1, v2, 'w'] = w
        return self._igraph
