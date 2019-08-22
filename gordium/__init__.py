from ._backend import BoundingBox, DataFrame, GraphBackend
from .networkx_backend import NetworkXBackend
from .neuprint_backend import NeuPrintBackend
try:
    import graph_tool
    from .graph_tool_backend import GraphToolBackend
except ModuleNotFoundError:
    pass
try:
    import igraph
    from .igraph_backend import IGraphBackend
except ModuleNotFoundError:
    pass

class Gordium():

    def __init__(
            self,
            edgeframe:DataFrame,
            backend:GraphBackend):
        self.graph = backend(edgeframe)
        self.fns = [
                self.graph.number_of_nodes,
                self.graph.number_of_edges,
                self.graph.number_of_loops,
                self.graph.number_of_leaves,
                self.graph.number_of_nodes_with_degree_over_1000,
                self.graph.max_degree,
                self.graph.mean_degree,
                self.graph.number_of_orphans,
                self.graph.number_of_lone_pairs,
                self.graph.max_strongly_connected_component_order,
                self.graph.max_weakly_connected_component_order,
        ]

    def process(
                self,
                bounding_box:BoundingBox=None) -> DataFrame:
        analytics = list()
        analytic = dict()
        for fn in self.fns:
            analytic[fn.__name__] = fn(bounding_box)
        analytics.append(analytic)
        analytics = DataFrame(analytics)
        return analytics

