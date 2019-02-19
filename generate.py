import networkx as nx
from networkx.generators.random_graphs import watts_strogatz_graph
from numpy.random import randint

def set_edge_positions(graph, positions):
    for coord_ix, coord in enumerate(('x', 'y', 'z')):
        position_map = {
                edge_id: positions[edge_ix, coord_ix]
                for edge_ix, edge_id
                in enumerate(graph.edges())}
        nx.set_edge_attributes(
                graph,
                position_map,
                coord)
    return

if __name__ == '__main__':
    graph = watts_strogatz_graph(1_000, 200, 0.1)
    positions = randint(0, 100_000, size=(graph.size(), 3))
    set_edge_positions(graph, positions)
    nx.write_graphml(graph, 'test.graphml.gz')

