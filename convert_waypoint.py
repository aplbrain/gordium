import glob
import json
import networkx as nx

if __name__ == '__main__':
    graph_paths = glob.glob('data/*')
    for graph_path in graph_paths:
        raw = json.load(open(graph_path, 'r'))
        graph = nx.DiGraph()
        edges = list()
        assert(len(raw['neuron_1']) == len(raw['neuron_2']))
        n_edges = len(raw['neuron_1'])
        for edge_ix in range(n_edges):
            from_node = raw['neuron_1'][edge_ix]
            to_node = raw['neuron_2'][edge_ix]
            coords = dict()
            for coord_name in ('x', 'y', 'z'):
                coords[coord_name] = raw['synapse_center'][coord_name][edge_ix]
            edge = (from_node, to_node, coords)
            edges.append(edge)
        graph.add_edges_from(edges)
        base_name = '.'.join(graph_path.split('/')[-1].split('.')[:-1])
        graph_path = 'data/{}.gz'.format(base_name)
        nx.write_graphml(graph, graph_path)

