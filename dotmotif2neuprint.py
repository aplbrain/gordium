import json

import dask.dataframe as dd
import numpy as np
import pandas as pd

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
    print('begin!')
    print('writing neurons...!')
    # transport neurons
    neurons = dd.read_csv('{}/export-neurons-0.csv'.format(dotmotif_dir))
    neurons.rename(columns={'neuronId:ID(Neuron)': 'Id'}).to_json('{}/neurons'.format(neuprint_dir))
    print('wrote neurons!')
    # transport synapses
    print('reading synapses...')
    synapses = dd.read_csv(
            '{}/export-synapses-*.csv'.format(dotmotif_dir),
            dtype=dtype_map).compute()
    print('read synapses!')
    print('arranging synapses...')
    seg_ids = np.union1d(synapses.presyn_segid.unique(), synapses.postsyn_segid.unique())
    pre_format = '{{"Type":"pre","Confidence":1.0,"Location":[{},{},{}],"ConnectsTo":[[{},{},{}]]}}'
    post_format = '{{"Type":"post","Confidence":1.0,"Location":[{},{},{}]}}'
    synapse_handle = open('{}/synapses/synapses.json'.format(neuprint_dir), 'w')
    synapse_handle.write('[') # bodies = list()
    for seg_ix, seg_id in enumerate(seg_ids):
        if seg_ix > 0:
            synapse_handle.write(',')
        if (seg_ix % 1e4 == 0):
            print('arranged {}/{} bodies!'.format(seg_ix, seg_ids.shape[0]))
        synapse_handle.write('{') # body = dict()
        synapse_handle.write('"BodyId:":{}'.format(seg_id)) # body["BodyId"] = seg_id
        synapse_handle.write(',')
        synapse_handle.write('"SynapseSet":[') # body["SynapseSet"] = list()
        is_pre = synapses.presyn_segid == seg_id
        is_post = synapses.postsyn_segid == seg_id
        subsyn = synapses[is_pre | is_post]
        for r_ix, row in subsyn.iterrows():
            if r_ix > 0:
                synapse_handle.write(',')
            if row.presyn_segid == seg_id:
                presyn = pre_format.format(
                        row.presyn_x,
                        row.presyn_y,
                        row.presyn_z,
                        row.postsyn_x,
                        row.postsyn_y,
                        row.postsyn_z)
                synapse_handle.write(presyn) # body["SynapseSet"].append(presyn)
            if row.presyn_segid == seg_id and row.postsyn_segid == seg_id:
                synapse_handle.write(',')
            if row.postsyn_segid == seg_id:
                postsyn = post_format.format(
                        row.postsyn_x,
                        row.postsyn_y,
                        row.postsyn_z)
                synapse_handle.write(postsyn) # body["SynapseSet"].append(postsyn)
        synapse_handle.write(']')
        synapse_handle.write('}') # bodies.append(body)
    synapse_handle.write(']')
    print('arranged synapses!')
    print('writing synapses...')
    # synapse_handle = open('{}/synapses/synapses.json'.format(neuprint_dir), 'w')
    # json.dump(bodies, synapse_handle)
    print('wrote synapses!')
    print('end!')

