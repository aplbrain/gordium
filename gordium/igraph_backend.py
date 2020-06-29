from typing import Union, Iterable
import igraph
from pandas import DataFrame

from ._backend import GraphBackend

class IGraphBackend(GraphBackend):

    def __init__(
            self,
            edgeframe:DataFrame,
            src_label:str,
            tgt_label:str):
        self._graph = igraph.Graph.TupleList(
                edgeframe[[
                        src_label,
                        tgt_label]].drop_duplicates().itertuples(index=False),
                directed=True)
        self._dh = None
        self._scch = None
        self._wcch = None

    def get_graph(self):
        return self._graph

    def number_of_nodes(self):
        return self._graph.vcount()

    def number_of_edges(self):
        return self._graph.ecount()

    def number_of_loops(self):
        return sum(self._graph.is_loop())
    
    def k_core(self, k: Union[int, Iterable[int]]):
        return self._graph.k_core(k)

    def degree_histogram(self):
        if self._dh is None:
            self._dh = DataFrame(
                    self._graph.degree_distribution().bins(),
                    columns=["degree", "_degree", "frequency"])
            self._dh = self._dh[self._dh.frequency > 0]
            self._dh = self._dh[["degree", "frequency"]]
            self._dh = self._dh.set_index("degree").frequency
        return self._dh

    def scc_histogram(self):
        if self._scch is None:
            self._scch = list(self._graph.components(igraph.STRONG).membership)
            self._scch = DataFrame(self._scch, columns=["cc"]).cc.value_counts().value_counts()
        return self._scch

    def wcc_histogram(self):
        if self._wcch is None:
            self._wcch = list(self._graph.components(igraph.WEAK).membership)
            self._wcch = DataFrame(self._wcch, columns=["cc"]).cc.value_counts().value_counts()
        return self._wcch

