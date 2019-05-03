import networkx as nx

from ._backend import BoundingBox, DataFrame, GraphBackend

class NetworkXBackend(GraphBackend):

    def __init__(self, edgeframe:DataFrame):
        self._graph = nx.from_pandas_edgelist(
                edgeframe,
                source='presyn_segid',
                target='postsyn_segid',
                edge_attr=True,
                create_using=nx.DiGraph)
        self._degree = None

    def number_of_nodes(self):
        return self._graph.order()

    def number_of_edges(self):
        return self._graph.size()

    def number_of_loops(self):
        return len(list(graph.selfloop_edges()))

    def degree_histogram(self):
        if self._degree is None:
            self._degree = pd.DataFrame(
                    self._graph.degree(),
                    columns=['n_id', 'degree']).degree.value_counts()
        return self._degree

    def number_of_orphans(self):
        return len(list(component for component in nx.weakly_connected_components(self._graph) if len(component) == 1))

    def number_of_lone_pairs(self):
        return len(list(component for component in nx.weakly_connected_components(self._graph) if len(component) == 2))

    def max_strongly_connected_component_order(self):
        return max(len(component) for component in nx.strongly_connected_components(self._graph))

    def max_weakly_connected_component_order(self):
        return max(len(component) for component in nx.weakly_connected_components(self._graph))

