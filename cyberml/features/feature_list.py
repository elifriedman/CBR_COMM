from .feature import Featurizer, StatsFeaturizer


@Featurizer(input_type='igraph')
def transitivity(graph):
    return graph.transitivity_avglocal_undirected(weights='w')

@StatsFeaturizer(input_type='igraph')
def strength(graph):
    return graph.strength(weights='w')

@Featurizer(input_type='igraph')
def num_nodes(graph):
    return graph.vcount()

@Featurizer(input_type='igraph')
def num_edges(graph):
    return graph.ecount()

@Featurizer(input_type='igraph')
def diameter(graph):
    return graph.diameter(directed=False)

@StatsFeaturizer(input_type='igraph')
def betweenness(graph):
    return graph.betweenness(vertices=None, directed=False)

@StatsFeaturizer(input_type='igraph')
def cluster_size(graph):
    blocks = graph.blocks(True)
    return [len(x) for x in blocks[0]._clusters]

@StatsFeaturizer(input_type='igraph')
def degree(graph):
    return graph.degree()

@Featurizer(input_type='igraph')
def density(graph):
    return graph.density()

def featurize(featurizer_list, graph):
    features = []
    for featurizer in featurizer_list:
        feat = featurizer(graph)
        if feat.type == 'vector':  # feat contains a list of features
            features.extend(feat.value)
        else:  # feat is just a single feature
            features.append(feat)
    return features


featurizer_list = [
    transitivity,
    strength,
    num_nodes,
    num_edges,
    diameter,
    betweenness,
    cluster_size,
    degree,
    density,
]

