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
    def number_of_nodes(self, bounding_box:BoundingBox=None):
        pass

    @abstractmethod
    def number_of_edges(self, bounding_box:BoundingBox=None):
        pass

    @abstractmethod
    def number_of_loops(self, bounding_box:BoundingBox=None):
        pass

    @abstractmethod
    def degree_histogram(self, bounding_box:BoundingBox=None):
        pass

    def number_of_leaves(self, bounding_box:BoundingBox=None):
        dh = self.degree_histogram(bounding_box)
        return dh.get(key=1, default=0)

    def number_of_nodes_with_degree_over_1000(self, bounding_box:BoundingBox=None):
        dh = self.degree_histogram(bounding_box)
        return dh[dh.index > 1000].sum()

    def max_degree(self, bounding_box:BoundingBox=None):
        dh = self.degree_histogram(bounding_box)
        order = dh.sum()
        return 0 if order==0 else dh.index.max()

    def mean_degree(self, bounding_box:BoundingBox=None):
        dh = self.degree_histogram(bounding_box)
        order = dh.sum()
        return 0 if order==0 else dh.dot(dh.index)/order

    @abstractmethod
    def in_degree_histogram(self, bounding_box:BoundingBox=None):
        pass

    def number_of_sources(self, bounding_box:BoundingBox=None):
        idh = self.in_degree_histogram(bounding_box)
        order = idh.sum()
        return 0 if order==0 else idh.get(key=0, default=0)

    @abstractmethod
    def out_degree_histogram(self, bounding_box:BoundingBox=None):
        pass

    def number_of_sinks(self, bounding_box:BoundingBox=None):
        odh = self.out_degree_histogram(bounding_box)
        order = odh.sum()
        return 0 if order==0 else odh.get(key=0, default=0)

    @abstractmethod
    def scc_histogram(self, bounding_box:BoundingBox=None):
        pass

    def max_strongly_connected_component_order(self, bounding_box:BoundingBox=None):
        scch = self.scc_histogram(bounding_box)
        order = scch.sum()
        return 0 if order==0 else scch.index.max()

    @abstractmethod
    def wcc_histogram(self, bounding_box:BoundingBox=None):
        pass

    def number_of_orphans(self, bounding_box:BoundingBox=None):
        wcch = self.wcc_histogram(bounding_box)
        return wcch.get(key=1, default=0)

    def number_of_lone_pairs(self, bounding_box:BoundingBox=None):
        wcch = self.wcc_histogram(bounding_box)
        return wcch.get(key=2, default=0)

    def max_weakly_connected_component_order(self, bounding_box:BoundingBox=None):
        wcch = self.wcc_histogram(bounding_box)
        order = wcch.sum()
        return 0 if order==0 else wcch.index.max()

