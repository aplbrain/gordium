import glob
import networkx as nx
import pandas as pd

from networkx.generators.directed import scale_free_graph

def rename(function, name):
    function.__name__ = name
    return function

def two_core_order(graph):
    two_core = graph.copy()
    two_core.remove_edges_from(two_core.selfloop_edges())
    two_core = nx.k_core(two_core, 2)
    return two_core.order()

functions = [
        rename(
            lambda graph: graph.order(),
            'number_of_nodes'),
        rename(
            lambda graph: graph.size(),
            'number_of_edges'),
        rename(
            lambda graph: len(list(graph.selfloop_edges())),
            'number_of_loops'),
        rename(
            lambda graph: len(list(degree for _, degree in graph.degree() if degree == 1)),
            'number_of_leaves'),
        rename(
            lambda graph: len(list(degree for _, degree in graph.degree() if degree > 1_000)),
            'number_of_nodes_with_degree_over_1000'),
        rename(
            lambda graph: max(degree for _, degree in graph.degree()),
            'max_degree'),
        rename(
            lambda graph: sum(degree for _, degree in graph.degree())/graph.order(),
            'mean_degree'),
        rename(
            lambda graph: len(list(component for component in nx.weakly_connected_components(graph) if len(component) == 1)),
            'number_of_orphans'),
        rename(
            lambda graph: len(list(component for component in nx.weakly_connected_components(graph) if len(component) == 2)),
            'number_of_lone_pairs'),
        two_core_order,
        rename(
            lambda graph: max(len(component) for component in nx.strongly_connected_components(graph)),
            'max_strongly_connected_component_order'),
        rename(
            lambda graph: max(len(component) for component in nx.weakly_connected_components(graph)),
            'max_weakly_connected_component_order'),
]

def analyze_graph(graph: nx.DiGraph) -> dict:
    result = dict()
    for function in functions:
        result[function.__name__] = function(graph)
    return result

def analyze_graphs(graphs: list) -> pd.DataFrame:
    results = list()
    for graph in graphs:
        result = analyze_graph(graph)
        results.append(result)
    table = pd.DataFrame(results)
    return table

if __name__ == '__main__':
    graph_paths = glob.glob('data/*')
    graphs = [nx.read_graphml(graph_path) for graph_path in graph_paths]
    table = analyze_graphs(graphs)
    function_names = [function.__name__ for function in functions]
    graph_names = ['.'.join(graph_path.split('/')[-1].split('.')[:-1]) for graph_path in graph_paths]
    table.rename(
            index={graph_ix: graph_name for graph_ix, graph_name in enumerate(graph_names)},
            inplace=True)
    table[function_names].to_csv('table.csv', index=True)
    print(table[function_names].T)

