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
                self.number_of_orphans,
                self.number_of_loops,
                self.number_of_lone_pairs,
                self.number_of_leaves,
                self.number_of_nodes_with_degree_over_1000,
                self.max_degree,
                self.mean_degree,
                self.strongly_connected_components,
                self.weakly_connected_components,
        ]

    def process(self) -> DataFrame:
        analytics = list()
        analytic = dict()
        for fn in self.fns:
            analytic[fn.__name__] = fn()
        analytics.append(analytic)
        analytics = DataFrame(analytics)
        return analytics

    def number_of_nodes(
            self,
            bounding_box:BoundingBox=None) -> int:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
        query += """
        MATCH (n:{label}Neuron)
        WITH count(n) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def number_of_edges(
            self,
            bounding_box:BoundingBox=None) -> int:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
        query += """
        MATCH (n)-[r]->()
        WITH count(r) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def number_of_orphans(
            self,
            bounding_box:BoundingBox=None) -> int:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
        query += """
        MATCH (n)
        WHERE not (n)-[*]-()
        WITH count(n) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def number_of_loops(
            self,
            bounding_box:BoundingBox=None) -> int:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
        query += """
        MATCH (n)-[r]->(n)
        WITH count(n) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def number_of_lone_pairs(self) -> int:
        query:str = """
        CALL algo.unionFind.stream('Neuron', 'SYN')
        YIELD nodeId, setId
        WITH setId, count(nodeId) as order_of_component
        WHERE order_of_component = 2
        WITH count(order_of_component) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def number_of_leaves(
            self,
            bounding_box:BoundingBox=None) -> int:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
        query += """
        MATCH (n)-[r]-()
        WITH n, count(r) as degree
        WHERE degree = 1
        WITH count(n) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def number_of_nodes_with_degree_over_1000(
            self,
            bounding_box:BoundingBox=None) -> int:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
        query += """
        MATCH (n)-[r]-()
        WITH n, count(r) as degree
        WHERE degree > 1000
        WITH count(n) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def max_degree(
            self,
            bounding_box:BoundingBox=None) -> int:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
        query += """
        MATCH (n)-[r]-()
        WITH n, count(r) as degree
        WITH max(degree) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def mean_degree(
            self,
            bounding_box:BoundingBox=None) -> float:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
        query += """
        MATCH (n)-[r]-()
        WITH n, count(r) as degree
        WITH avg(degree) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def max_strongly_connected_components_order(self) -> int:
        query:str = """
        CALL algo.scc('Neuron', 'SYN')
        YIELD maxSetSize
        WITH maxSetSize AS metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def max_weakly_connected_components_order(self) -> int:
        query:str = """
        CALL algo.unionFind.stream('Neuron', 'SYN')
        YIELD nodeId, setId
        WITH setId, count(nodeId) as order_of_component
        WITH max(order_of_component) as metric
        RETURN metric;
        """
        return self._compute_metric(query)

    def _spatial_subset(self, bounding_box:BoundingBox) -> str:
        query:str = """
        MATCH (cs:ConnectionSet)-[:Contains]->(s:Synapse)
        WHERE point({{x:{},y:{},z:{}}}) <= s.location < point({{x:{},y:{},z:{}}})
        WITH DISTINCT cs
        MATCH (n0:Neuron)<-[:From]-(cs)-[:To]->(n1:Neuron)
        MATCH DISTINCT (n0)-[:ConnectsTo]->(n1)
        """.format(
                bounding_box.x_lower,
                bounding_box.y_lower,
                bounding_box.z_lower,
                bounding_box.x_upper,
                bounding_box.y_upper,
                bounding_box.z_upper)
        return query

    def _spatial_node_query(self, bounding_box:BoundingBox) -> str:
        query:str = """
        MATCH (n:Neuron)<-[:From|:To]-(cs:ConnectionSet)-[:Contains]->(s:Synapse)
        WHERE point({{x:{},y:{},z:{}}}) <= s.location < point({{x:{},y:{},z:{}}})
        RETURN id(n) AS id;
        """.format(
                bounding_box.x_lower,
                bounding_box.y_lower,
                bounding_box.z_lower,
                bounding_box.x_upper,
                bounding_box.y_upper,
                bounding_box.z_upper)
        return query

    def _spatial_relationship_query(self, bounding_box:BoundingBox) -> str:
        query:str = """
        MATCH (cs:ConnectionSet)-[:Contains]->(s:Synapse)
        WHERE point({{x:{},y:{},z:{}}}) <= s.location < point({{x:{},y:{},z:{}}})
        WITH DISTINCT cs
        MATCH (n0:Neuron)<-[:From]-(cs)-[:To]->(n1:Neuron)
        RETURN DISTINCT id(n0) AS source, id(n1) AS target;
        """.format(
                bounding_box.x_lower,
                bounding_box.y_lower,
                bounding_box.z_lower,
                bounding_box.x_upper,
                bounding_box.y_upper,
                bounding_box.z_upper)
        return query

    def _compute_metric(self, query):
        return self.neo4j.run(query).to_data_frame().metric[0]

