import igraph

from ._backend import BoundingBox, DataFrame, GraphBackend

class IGraphBackend(GraphBackend):

    def __init__(
            self,
            edgeframe:DataFrame):
        self._graph = igraph.Graph.TupleList(
                edgeframe[[
                        "presyn_segid",
                        "postsyn_segid"]].drop_duplicates().itertuples(index=False),
                directed=True)
        self._dh = None
        self._scch = None
        self._wcch = None

    def number_of_nodes(
            self,
            bounding_box:BoundingBox=None):
        return self._graph.vcount()

    def number_of_edges(
            self,
            bounding_box:BoundingBox=None):
        return self._graph.ecount()

    def number_of_loops(
            self,
            bounding_box:BoundingBox=None):
        return sum(self._graph.is_loop())

    def degree_histogram(
            self,
            bounding_box:BoundingBox=None):
        if self._dh is None:
            self._dh = DataFrame(
                    self._graph.degree_distribution().bins(),
                    columns=["degree", "_degree", "frequency"])
            self._dh = self._dh[self._dh.frequency > 0]
            self._dh = self._dh[["degree", "frequency"]]
            self._dh = self._dh.set_index("degree").frequency
        return self._dh

    def scc_histogram(
            self,
            bounding_box:BoundingBox=None):
        if self._scch is None:
            self._scch = list(self._graph.components(igraph.STRONG).membership)
            self._scch = DataFrame(self._scch, columns=["cc"]).cc.value_counts().value_counts()
        return self._scch

    def wcc_histogram(
            self,
            bounding_box:BoundingBox=None):
        if self._wcch is None:
            self._wcch = list(self._graph.components(igraph.WEAK).membership)
            self._wcch = DataFrame(self._wcch, columns=["cc"]).cc.value_counts().value_counts()
        return self._wcch

