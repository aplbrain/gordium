import networkx as nx

from ._backend import BoundingBox, DataFrame, GraphBackend

class NetworkXBackend(GraphBackend):

    def __init__(self, edgeframe:DataFrame):
        self._graph = nx.from_pandas_edgelist(
                edgeframe,
                source="presyn_segid",
                target="postsyn_segid",
                edge_attr=True,
                create_using=nx.DiGraph)
        self._dh = None
        self._scch = None
        self._wcch = None

    def number_of_nodes(self):
        return self._graph.order()

    def number_of_edges(self):
        return self._graph.size()

    def number_of_loops(self):
        return len(list(self._graph.selfloop_edges()))

    def degree_histogram(self):
        if self._dh is None:
            self._dh = DataFrame(
                    self._graph.degree(),
                    columns=["n_id", "degree"]).degree.value_counts()
        return self._dh

    def scc_histogram(self):
        if self._scch is None:
            self._scch = [len(cc) for cc in nx.strongly_connected_components(self._graph)]
            self._scch = DataFrame(self._scch, columns=["cc_order"]).cc_order.value_counts()
        return self._scch

    def wcc_histogram(self):
        if self._wcch is None:
            self._wcch = [len(cc) for cc in nx.weakly_connected_components(self._graph)]
            self._wcch = DataFrame(self._wcch, columns=["cc_order"]).cc_order.value_counts()
        return self._wcch

