from hashlib import md5
from numpy import union1d
from pandas import DataFrame, read_csv


if __name__ == '__main__':

    export_dir = 'import'
    print('reading...')
    df = read_csv('team_2_phase_1_centroid.csv')

    print('cleaning...')
    df = df.dropna()
    df = df.drop_duplicates()

    dtypes = {
            'presyn_segid': 'uint64',
            'postsyn_segid': 'uint64',
            'centroid_x': 'int32',
            'centroid_y': 'int32',
            'centroid_z': 'int32',
            # 'presyn_x': 'int32',
            # 'presyn_y': 'int32',
            # 'presyn_z': 'int32',
            # 'postsyn_x': 'int32',
            # 'postsyn_y': 'int32',
            # 'postsyn_z': 'int32'
            }
    df = df.astype(dtypes)
    # df['centroid_x'] = (0.5*(df.presyn_x + df.postsyn_x)).astype('int32')
    # df['centroid_y'] = (0.5*(df.presyn_y + df.postsyn_y)).astype('int32')
    # df['centroid_z'] = (0.5*(df.presyn_z + df.postsyn_z)).astype('int32')

    # segments
    print('neurons...')
    neurons = DataFrame()
    neurons['neuid:ID'] = union1d(df.presyn_segid.unique(), df.postsyn_segid.unique())
    neurons[':LABEL'] = 'Neuron'
    neurons.to_csv('{}/neurons.csv'.format(export_dir), index=False)

    # synapses
    print('synapses...')
    synapses = DataFrame()
    synapses['location:point'] = df.apply(lambda r: '{{x:{},y:{},z:{}}}'.format(r.centroid_x, r.centroid_y, r.centroid_z), axis=1)
    # synapses['synid:ID'] = synapses['location:point'].map(lambda s: md5(s.encode()).hexdigest())
    synapses['synid:ID'] = synapses['location:point'].str[1:-1]
    synapses[':LABEL'] = 'Synapse'
    synapses.to_csv('{}/synapses.csv'.format(export_dir), index=False)

    # _connects_to
    print('connects to...')
    _connects_to = DataFrame()
    _connects_to[':START_ID'] = df.presyn_segid
    _connects_to[':END_ID'] = df.postsyn_segid
    _connects_to[':TYPE'] = 'ConnectsTo'
    _connects_to = _connects_to.drop_duplicates()
    _connects_to.to_csv('{}/_connects_to.csv'.format(export_dir), index=False)

    # _from
    print('from...')
    _from = DataFrame()
    _from[':START_ID'] = synapses['synid:ID']
    _from[':END_ID'] = df.presyn_segid
    _from[':TYPE'] = 'From'
    _from.to_csv('{}/_from.csv'.format(export_dir), index=False)

    # _to
    print('to...')
    _to = DataFrame()
    _to[':START_ID'] = synapses['synid:ID']
    _to[':END_ID'] = df.postsyn_segid
    _to[':TYPE'] = 'To'
    _to.to_csv('{}/_to.csv'.format(export_dir), index=False)

