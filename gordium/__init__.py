from pandas import DataFrame
from ._backend import GraphBackend
from .networkx_backend import NetworkXBackend
try:
    import igraph
    from .igraph_backend import IGraphBackend
except ModuleNotFoundError:
    pass

class Gordium():

    def __init__(
            self,
            edgeframe:DataFrame,
            backend:GraphBackend=NetworkXBackend,
            src_label:str="source",
            tgt_label:str="target"):
        self._backend = backend(
                edgeframe,
                src_label,
                tgt_label)
        self.fns = [
                self._backend.number_of_nodes,
                self._backend.number_of_edges,
                self._backend.number_of_loops,
                self._backend.number_of_leaves,
                self._backend.number_of_nodes_with_degree_over_1000,
                self._backend.max_degree,
                self._backend.mean_degree,
                self._backend.number_of_orphans,
                self._backend.number_of_lone_pairs,
                self._backend.max_strongly_connected_component_order,
                self._backend.max_weakly_connected_component_order,
        ]

    def process(self) -> DataFrame:
        analytics = list()
        analytic = dict()
        for fn in self.fns:
            analytic[fn.__name__] = fn()
        analytics.append(analytic)
        analytics = DataFrame(analytics)
        return analytics

    def get_graph(self) -> object:
        return self._backend.get_graph()
