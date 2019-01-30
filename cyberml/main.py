import os
import argparse
import time

from .graph import Graph
from .features.feature_list import featurizer_list, featurize


def main():
    parseargs = argparse.ArgumentParser()
#    parseargs.add_argument("filename", type=str, help="add an input graph csv file in edge list format")
    parseargs.add_argument("-o", "--output", type=str, default="output.csv", help="add an output file name")
    args = parseargs.parse_args()

    dirn = "_graphs/"
    graphnames = os.listdir(dirn)
    f = open(args.output, 'w')
    n = 0
    for i, graphname in enumerate(graphnames):
        print("processing graph {}/{}".format(i, len(graphnames)), end=", ")
        sl = time.time()
        graph = Graph.load_from_csv(dirn+graphname)
        print("load={}".format(time.time() - sl), end=", ")
        sf = time.time()
        features = featurize(featurizer_list, graph)
        print("feat={}".format(time.time() - sf))
        names = ['footprint_id'] + [feat.name for feat in features]
        vals = [feat.value for feat in features]
        if i == 0:
            f.write(",".join(names)+"\n")
        f.write(graphname+"," + ",".join(["%.3f" % val for val in vals]) + "\n")
        f.flush()
    f.close()


if __name__ == '__main__':
    main()
