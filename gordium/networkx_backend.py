import networkx as nx

from ._backend import BoundingBox, DataFrame, GraphBackend

class NetworkXBackend(GraphBackend):

    def __init__(
            self,
            edgeframe:DataFrame):
        self._graph = nx.from_pandas_edgelist(
                edgeframe,
                source="presyn_segid",
                target="postsyn_segid",
                edge_attr=True,
                create_using=nx.DiGraph)
        self._dh = None
        self._idh = None
        self._odh = None
        self._scch = None
        self._wcch = None

    def number_of_nodes(
            self,
            bounding_box:BoundingBox=None):
        return self._graph.order()

    def number_of_edges(
            self,
            bounding_box:BoundingBox=None):
        return self._graph.size()

    def number_of_loops(
            self,
            bounding_box:BoundingBox=None):
        return len(list(self._graph.selfloop_edges()))

    def degree_histogram(
            self,
            bounding_box:BoundingBox=None):
        if self._dh is None:
            self._dh = DataFrame(
                    self._graph.degree(),
                    columns=["n_id", "degree"]).degree.value_counts()
        return self._dh

    def in_degree_histogram(
            self,
            bounding_box:BoundingBox=None):
        if self._idh is None:
            self._idh = DataFrame(
                    self._graph.in_degree(),
                    columns=["n_id", "in_degree"]).in_degree.value_counts()
        return self._idh

    def out_degree_histogram(
            self,
            bounding_box:BoundingBox=None):
        if self._odh is None:
            self._odh = DataFrame(
                    self._graph.out_degree(),
                    columns=["n_id", "out_degree"]).out_degree.value_counts()
        return self._odh

    def scc_histogram(
            self,
            bounding_box:BoundingBox=None):
        if self._scch is None:
            self._scch = [len(cc) for cc in nx.strongly_connected_components(self._graph)]
            self._scch = DataFrame(self._scch, columns=["cc_order"]).cc_order.value_counts()
        return self._scch

    def wcc_histogram(
            self,
            bounding_box:BoundingBox=None):
        if self._wcch is None:
            self._wcch = [len(cc) for cc in nx.weakly_connected_components(self._graph)]
            self._wcch = DataFrame(self._wcch, columns=["cc_order"]).cc_order.value_counts()
        return self._wcch

