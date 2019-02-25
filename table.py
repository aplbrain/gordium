import networkx as nx
import pandas as pd

from networkx.generators.directed import scale_free_graph

def rename(function, name):
    function.__name__ = name
    return function

functions = [
        rename(
            lambda graph: graph.order(),
            'nodes'),
        rename(
            lambda graph: graph.size(),
            'edges'),
        rename(
            lambda graph: len(list(component for component in nx.weakly_connected_components(graph) if len(component) == 1)),
            'number_of_orphans'),
        rename(
            lambda graph: len(list(component for component in nx.weakly_connected_components(graph) if len(component) == 2)),
            'number_of_lone_pairs'),
        rename(
            lambda graph: len(list(degree for _, degree in graph.degree() if degree == 1)),
            'number_of_leaves'),
        rename(
            lambda graph: len(list(degree for _, degree in graph.degree() if degree > 1_000)),
            'number_of_degree_over_1000'),
        rename(
            lambda graph: sum(degree for _, degree in graph.degree())/graph.order(),
            'mean_degree'),
        rename(
            lambda graph: max(degree for _, degree in graph.degree()),
            'max_degree'),
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
    graphs = [scale_free_graph(1_000)]
    table = analyze_graphs(graphs)
    function_names = [function.__name__ for function in functions]
    print(table[function_names].T)

