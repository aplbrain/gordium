from pandas import DataFrame
import dotmotif

class Gordium():

    def __init__(self, graph: str):
        self.dm_conn = None
        self.fns = [
                self.strongly_connected_components,
                # self.weakly_connected_components,
                # self.number_of_nodes,
                # self.number_of_edges,
                # self.number_of_orphans,
                # self.number_of_loops,
                # self.number_of_lone_pairs,
                # self.number_of_leaves,
                # self.number_of_nodes_with_degree_gt_1000,
                # self.mean_degree,
                # self.max_degree,
        ]

    def process(self):
        analytics = list()
        for fn in self.fns:
            analytic = dict()
            analytic[fn.__name__] = fn()
            analytics.append(analytic)
        analytics = DataFrame(analytics)
        return analytics

    def strongly_connected_components(self):
        return 0

