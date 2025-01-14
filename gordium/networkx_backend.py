from typing import Union, Iterable
import networkx as nx
from pandas import DataFrame

from ._backend import GraphBackend

class NetworkXBackend(GraphBackend):

    def __init__(
            self,
            edgeframe:DataFrame,
            src_label:str,
            tgt_label:str):
        self._graph = nx.from_pandas_edgelist(
                edgeframe,
                source=src_label,
                target=tgt_label,
                create_using=nx.DiGraph)
        self._dh = None
        self._scch = None
        self._wcch = None

    def get_graph(self):
        return self._graph

    def number_of_nodes(self):
        return self._graph.order()

    def number_of_edges(self):
        return self._graph.size()

    def number_of_loops(self):
        return len(list(nx.selfloop_edges(self._graph)))

    def k_core(self, k: Union[int, Iterable[int]]):
        if isinstance(k, int):
            return nx.k_core(self._graph, k)
        if isinstance(k, Iterable):
            return [
                nx.k_core(self._graph, k_num)
                for k_num in k
            ]

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

