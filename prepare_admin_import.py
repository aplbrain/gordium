from multiprocessing import cpu_count
from sys import argv

import dask.dataframe as dd

from dask.dataframe import read_csv
# from dask.distributed import Client
from numpy import union1d
from pandas import DataFrame

def prepare_admin_import(df, export_dir):
    # client = Client()

    # export_dir = 'import'
    print('cleaning...')
    df = df.repartition(npartitions=cpu_count())
    df = df.dropna()
    df = df.drop_duplicates()

    dtypes = {
            'presyn_segid': 'uint64',
            'postsyn_segid': 'uint64',
            'centroid_x': 'int32',
            'centroid_y': 'int32',
            'centroid_z': 'int32',
            }
    df = df.astype(dtypes)
    presyn_segid = df.presyn_segid.compute()
    postsyn_segid = df.postsyn_segid.compute()

    # segments
    print('neurons...')
    neurons = DataFrame()
    neurons['neuid:ID'] = union1d(presyn_segid.unique(), postsyn_segid.unique())
    neurons[':LABEL'] = 'Neuron'
    neurons.to_csv('{}/neurons.csv'.format(export_dir), index=False)
    neurons = None

    # synapses
    print('synapses...')
    synapses = DataFrame()
    synapses['location:point'] = df.map_partitions(
            lambda df: df.apply(
                    lambda r: '{{x:{},y:{},z:{}}}'.format(r.centroid_x, r.centroid_y, r.centroid_z),
                    axis=1)).compute()
    # synapses['synid:ID'] = synapses['location:point'].map(lambda s: md5(s.encode()).hexdigest())
    synapses['synid:ID'] = synapses['location:point'].str[1:-1]
    synapses[':LABEL'] = 'Synapse'
    synapses.to_csv('{}/synapses.csv'.format(export_dir), index=False)

    # connection_sets
    print('connection sets...')
    connection_sets = DataFrame()
    connection_sets['csid:ID'] = df.map_partitions(
            lambda df: df.apply(
                    lambda r: '{}:{}'.format(r.presyn_segid, r.postsyn_segid),
                    axis=1)).compute()
    connection_sets[':LABEL'] = 'ConnectionSet'

    # _connects_to
    print('connects to...')
    _connects_to = DataFrame()
    _connects_to[':START_ID'] = presyn_segid
    _connects_to[':END_ID'] = postsyn_segid
    _connects_to[':TYPE'] = 'ConnectsTo'
    _connects_to = _connects_to.drop_duplicates()
    _connects_to.to_csv('{}/_connects_to.csv'.format(export_dir), index=False)
    _connects_to = None

    # _from
    print('from...')
    _from = DataFrame()
    _from[':START_ID'] = connection_sets['csid:ID']
    _from[':END_ID'] = presyn_segid
    _from[':TYPE'] = 'From'
    _from.to_csv('{}/_from.csv'.format(export_dir), index=False)
    _from = None

    # _to
    print('to...')
    _to = DataFrame()
    _to[':START_ID'] = connection_sets['csid:ID']
    _to[':END_ID'] = postsyn_segid
    _to[':TYPE'] = 'To'
    _to.to_csv('{}/_to.csv'.format(export_dir), index=False)
    _to = None

    # free up df
    df = None

    # _contains
    print('contains...')
    _contains = DataFrame()
    _contains[':START_ID'] = connection_sets['csid:ID']
    _contains[':END_ID'] = synapses['synid:ID']
    _contains[':TYPE'] = 'Contains'
    _contains = _contains.drop_duplicates()
    _contains.to_csv('{}/_contains.csv'.format(export_dir), index=False)
    _contains = None

    # free up synapses
    synapses = None

    # write connection sets to disk
    connection_sets = connection_sets.drop_duplicates()
    connection_sets.to_csv(
            '{}/connection_sets.csv'.format(export_dir),
            index=False)
    connection_sets = None

    # client.close()
    return

if __name__ == '__main__':
    edges_file = argv[1]
    export_dir = argv[2]
    print('reading...')
    edgeframe = read_csv(edges_file)
    prepare_admin_import(edgeframe, export_dir)

