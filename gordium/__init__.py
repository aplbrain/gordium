from pandas import DataFrame
from dotmotif.executors import Neo4jExecutor

class Gordium():

    def __init__(
            self,
            db_bolt_uri:str,
            username:str='neo4j',
            password:str='neuprint'):
        self.neo4j = Neo4jExecutor(
                db_bolt_uri=db_bolt_uri,
                username=username,
                password=password)
        self.fns = [
                self.number_of_nodes,
                self.number_of_edges,
                # self.number_of_orphans,
                # self.number_of_loops,
                # self.number_of_lone_pairs,
                # self.number_of_leaves,
                # self.number_of_nodes_with_degree_gt_1000,
                # self.mean_degree,
                # self.max_degree,
                # self.strongly_connected_components,
                # self.weakly_connected_components,
        ]

    def process(self) -> DataFrame:
        analytics = list()
        analytic = dict()
        for fn in self.fns:
            analytic[fn.__name__] = fn()
        analytics.append(analytic)
        analytics = DataFrame(analytics)
        return analytics

    def number_of_nodes(self) -> int:
        query:str = """
        MATCH (n)
        WITH count(n) as metric
        RETURN metric;
        """
        return self._extract_metric(self.neo4j.run(query))

    def number_of_edges(self) -> int:
        query:str = """
        MATCH (n)-[r]->()
        WITH count(r) as metric
        RETURN metric;
        """
        return self._extract_metric(self.neo4j.run(query))

    def strongly_connected_components(self):
        query:str = """
        CALL algo.scc('Letter', 'BIGRAM')
        YIELD maxSetSize
        WITH maxSetSize AS metric
        RETURN metric;
        """
        return self._extract_metric(self.neo4j.run(query))

    def _extract_metric(self, cursor):
        return cursor.to_data_frame().metric[0]

