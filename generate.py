from sys import argv

import dask.array as da
import dask.dataframe as dd

from dask.array import concatenate, unique
from dask.array.random import randint
from dask.dataframe import DataFrame
# from dask.distributed import Client

def generate(n_nodes):
    EDGE_FRACTION = 0.2
    # client = Client()
    n_edges_max = n_nodes * n_nodes # allow self-loops
    n_edges = int(n_edges_max*EDGE_FRACTION)
    edges = randint(0, n_nodes, size=(2*n_edges, 2))
    edges = dd.from_dask_array(
            edges,
            columns=['presyn_segid', 'postsyn_segid'])
    edges = edges.drop_duplicates()
    positions = randint(0, 1_000_000, size=(2*n_edges, 3))
    positions = dd.from_dask_array(
            positions,
            columns = ['centroid_x', 'centroid_y', 'centroid_z'])
    positions = positions.drop_duplicates()
    edges.repartition(npartitions=8)
    positions.repartition(npartitions=8)
    edges = edges.loc[:n_edges]
    positions = positions.loc[:edges.shape[0]-1]
    df = dd.concat([edges, positions], axis=1)
    # client.close()
    return df

if __name__ == '__main__':
    edges_file = argv[1]
    n_nodes = int(argv[2])
    df = generate(n_nodes)
    df.to_csv(edges_file, index=False)

