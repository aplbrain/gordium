import json

import dask.dataframe as dd

dtype_map = {
        ':START_ID(Neuron)': 'int64',
        ':END_ID(Neuron)': 'int64',
        'cleft_segid': 'float64',
        'presyn_segid': 'float64',
        'postsyn_segid': 'float64',
        'centroid_x': 'float64',
        'centroid_y': 'float64',
        'centroid_z': 'float64',
        'presyn_basin': 'float64',
        'presyn_x': 'float64',
        'presyn_y': 'float64',
        'presyn_z': 'float64',
        'postsyn_basin': 'float64',
        'postsyn_x': 'float64',
        'postsyn_y': 'float64',
        'postsyn_z': 'float64',
        'BBOX_bx': 'float64',
        'BBOX_by': 'float64',
        'BBOX_bz': 'float64',
        'BBOX_ex': 'float64',
        'BBOX_ey': 'float64',
        'BBOX_ez': 'float64',
        'size': 'float64'}

def _format_presyn(df):
    synapse_list = list()
    for r_ix, row in df.iterrows():
        synapse = dict()
        pre_location = [
                row.presyn_x,
                row.presyn_y,
                row.presyn_z]
        post_location = [
                row.postsyn_x,
                row.postsyn_y,
                row.postsyn_z]
        synapse['Type'] = 'pre'
        synapse['Location'] = pre_location
        synapse['ConnectsTo'] = [post_location]
        synapse_list.append(synapse)
    return synapse_list

def _format_postsyn(df):
    synapse_list = list()
    for r_ix, row in df.iterrows():
        synapse = dict()
        post_location = [
                row.postsyn_x,
                row.postsyn_y,
                row.postsyn_z]
        synapse['Type'] = 'post'
        synapse['Location'] = post_location
        synapse_list.append(synapse)
    return synapse_list

def transport_synapses(synapses):
    presyns = synapses.groupby(':START_ID(Neuron)').apply(_format_presyn)
    postsyns = synapses.groupby(':END_ID(Neuron)').apply(_format_postsyn)
    return (presyns, postsyns)

if __name__ == '__main__':
    dotmotif_dir = 'test_tiny'
    neuprint_dir = 'test_neuprint'
    # transport neurons
    neurons = dd.read_csv('{}/export-neurons-0.csv'.format(dotmotif_dir))
    neurons.rename(columns={'neuronId:ID(Neuron)': 'Id'}).to_json('{}/neurons'.format(neuprint_dir))
    # transport synapses
    synapses = dd.read_csv('{}/export-synapses-*.csv'.format(dotmotif_dir), dtype=dtype_map)
    presyns, postsyns = transport_synapses(synapses)
    synapse_handle = open('{}/synapses/synapses.json'.format(neuprint_dir), 'w')
    synapse_handle.write('[')
    pre_ids = set(presyns.index.compute())
    post_ids = set(postsyns.index.compute())
    neuron_ids = pre_ids | post_ids
    for n_ix, neuron in enumerate(neuron_ids):
        if n_ix == 0:
            synapse_set_json = ''
        else:
            synapse_set_json = ','
        synapse_set_json += '{"BodyId":'
        synapse_set_json += str(neuron)
        synapse_set_json += ',"SynapseSet":['
        if neuron in pre_ids:
            synapse_set_json += ','.join(dumps(pres) for pres in presyns.loc[neuron].item())
        if neuron in pre_ids and neuron in post_ids:
            synapse_set_json += ','
        if neuron in post_ids:
            synapse_set_json += ','.join(dumps(posts) for posts in postsyns.loc[neuron].item())
        synapse_set_json += ']}'
        synapse_handle.write(synapse_set_json)
    synapse_handle.write(']')

