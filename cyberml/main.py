import os
import argparse
import time

from .graph import Graph
from .features.feature_list import featurizer_list, featurize
from .train import train

def _load_graphs():
    dirn = "_graphs/"
    graphnames = os.listdir(dirn)
    return [Graph.load_from_csv(dirn+graphname) for graphname in graphnames]

def _update_features(output_file, graphs):
    f = open(output_file, 'w')
    for i, graph in enumerate(graphs):
        sf = time.time()
        features = featurize(featurizer_list, graph)
        print("feat={}".format(time.time() - sf))
        names = ['footprint_id'] + [feat.name for feat in features]
        vals = [feat.value for feat in features]
        if i == 0:
            f.write(",".join(names)+"\n")
        f.write(graphname+"," + ",".join([str(val) for val in vals]) + "\n")
        f.flush()
    f.close()

def main():
   parseargs = argparse.ArgumentParser()
   parseargs.add_argument("-o", "--output", type=str, default="output.csv", help="add an output file name")
   parseargs.add_argument("--features", action="store_true")
   args = parseargs.parse_args()

   graphs = _load_graphs()
   if args.features:
       _update_features(args.output, graphs)
   else:
       train(graphs)




if __name__ == '__main__':
    main()
