from abc import ABC, abstractmethod
from collections import namedtuple

from pandas import DataFrame

BoundingBox = namedtuple(
        "BoundingBox",
        "x_lower y_lower z_lower x_upper y_upper z_upper")

class GraphBackend(ABC):

    @abstractmethod
    def __init__(self, edgeframe:DataFrame):
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

    def number_of_leaves(self):
        dh = self.degree_histogram()
        return dh[1]

    def number_of_nodes_with_degree_over_1000(self):
        dh = self.degree_histogram()
        return dh[dh.index > 1000].sum()

    def max_degree(self):
        dh = self.degree_histogram()
        return dh.index.max()

    def mean_degree(self):
        dh = self.degree_histogram()
        return dh.dot(dh.index)/dh.sum()

    @abstractmethod
    def scc_histogram(self):
        pass

    def max_strongly_connected_component_order(self):
        scch = self.scc_histogram()
        return scch.index.max()

    @abstractmethod
    def wcc_histogram(self):
        pass

    def number_of_orphans(self):
        wcch = self.wcc_histogram()
        return wcch[1]

    def number_of_lone_pairs(self):
        wcch = self.wcc_histogram()
        return wcch[2]

    def max_weakly_connected_component_order(self):
        wcch = self.wcc_histogram()
        return wcch.index.max()

