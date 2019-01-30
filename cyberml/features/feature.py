import numpy as np


class Feature(object):
    def __init__(self, name, value, type):
        self.name = name
        self.value = value
        self.type = type

class Featurizer(object):
    def __init__(self, input_type=None, output_type='float'):
        self.input_type = input_type
        self.output_type = output_type

    def _get_input(self, graph):
        if self.input_type == 'networkx':
            g = graph.get_networkx()
        elif self.input_type == 'igraph':
            g = graph.get_igraph()
        else:
            g = graph
        return g

    def __call__(self, fn):
        name = fn.__name__
        def func(graph):
            graph = self._get_input(graph)
            value = fn(graph)
            return Feature(name, value, self.output_type)
        return func

class StatsFeaturizer(Featurizer):
    def __init__(self, input_type=None, stats=['max', 'min', 'mean', 'std', 'median']):
        super(StatsFeaturizer, self).__init__(input_type=input_type, output_type='vector')
        self.stats = stats

    def __call__(self, fn):
        name = fn.__name__
        def func(graph):
            features = []
            graph = self._get_input(graph)
            values = fn(graph)
            for stat in self.stats:
                if hasattr(np, stat):
                    stat_fn = getattr(np, stat)
                    feat_value = stat_fn(values)
                    feat_name = "{}_{}".format(name, stat)
                    features.append(Feature(feat_name, feat_value, 'float'))
            return Feature(name, features, 'vector')
        return func

