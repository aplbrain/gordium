from abc import ABC, abstractmethod
from collections import namedtuple

from pandas import DataFrame

class GraphBackend(ABC):

    @abstractmethod
    def __init__(
            self,
            edgeframe:DataFrame,
            src_label:str,
            tgt_label:str):
        pass

    @abstractmethod
    def get_graph(self) -> object:
        pass

    @abstractmethod
    def number_of_nodes(self):
        pass

    @abstractmethod
    def number_of_edges(self):
        pass

    @abstractmethod
    def number_of_loops(self):
        pass

    @abstractmethod
    def degree_histogram(self):
        pass

    @abstractmethod
    def k_core(self, k: int):
        pass

    def number_of_leaves(self):
        dh = self.degree_histogram()
        return dh.get(key=1, default=0)

    def number_of_nodes_with_degree_over_1000(self):
        dh = self.degree_histogram()
        return dh[dh.index > 1000].sum()

    def max_degree(self):
        dh = self.degree_histogram()
        order = dh.sum()
        return 0 if order==0 else dh.index.max()

    def mean_degree(self):
        dh = self.degree_histogram()
        order = dh.sum()
        return 0 if order==0 else dh.dot(dh.index)/order

    @abstractmethod
    def scc_histogram(self):
        pass

    def max_strongly_connected_component_order(self):
        scch = self.scc_histogram()
        order = scch.sum()
        return 0 if order==0 else scch.index.max()

    @abstractmethod
    def wcc_histogram(self):
        pass

    def number_of_orphans(self):
        wcch = self.wcc_histogram()
        return wcch.get(key=1, default=0)

    def number_of_lone_pairs(self):
        wcch = self.wcc_histogram()
        return wcch.get(key=2, default=0)

    def max_weakly_connected_component_order(self):
        wcch = self.wcc_histogram()
        order = wcch.sum()
        return 0 if order==0 else wcch.index.max()
