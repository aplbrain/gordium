from multiprocessing import cpu_count
from sys import argv

import dask.array as da
import dask.dataframe as dd

from dask.array import concatenate, unique
from dask.array.random import randint, seed
from dask.dataframe import DataFrame

seed(0)

def generate(n_nodes, edge_density=0.2):
    n_edges_max = n_nodes * n_nodes # allow self-loops
    n_edges = int(n_edges_max*edge_density)
    edges = randint(0, n_nodes, size=(n_edges, 2))
    edges = dd.from_dask_array(
            edges,
            columns=['presyn_segid', 'postsyn_segid'])
    edges = edges.drop_duplicates()
    positions = randint(0, 1_000_000, size=(n_edges, 3))
    positions = dd.from_dask_array(
            positions,
            columns = ['centroid_x', 'centroid_y', 'centroid_z'])
    positions = positions.drop_duplicates()
    edges = edges.loc[:(n_edges-1)]
    positions = positions.loc[:(edges.shape[0]-1)]
    edges.repartition(npartitions=cpu_count())
    positions.repartition(npartitions=cpu_count())
    df = dd.concat([edges, positions], axis=1)
    df = df.dropna()
    return df

if __name__ == '__main__':
    edges_file = argv[1]
    n_nodes = int(argv[2])
    df = generate(n_nodes)
    df.to_csv(edges_file, index=False)

