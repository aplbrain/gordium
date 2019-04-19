from collections import namedtuple

from pandas import DataFrame
from dotmotif.executors import Neo4jExecutor

BoundingBox = namedtuple(
        "BoundingBox",
        "x_lower y_lower z_lower x_upper y_upper z_upper")

class Gordium():

    def __init__(
            self,
            db_bolt_uri:str,
            username:str="neo4j",
            password:str="neuprint"):
        self.neo4j = Neo4jExecutor(
                db_bolt_uri=db_bolt_uri,
                username=username,
                password=password)
        self.fns = [
                self.number_of_nodes,
                self.number_of_edges,
                # self.number_of_loops,
                self.number_of_leaves,
                self.number_of_nodes_with_degree_over_1000,
                self.max_degree,
                self.mean_degree,
                self.number_of_orphans,
                self.number_of_lone_pairs,
                self.max_strongly_connected_components_order,
                self.max_weakly_connected_components_order,
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

    def number_of_nodes(
            self,
            bounding_box:BoundingBox=None) -> int:
        query:str = ""
        if bounding_box is not None:
            query += self._spatial_subset(bounding_box)
            query += " "
            query += """
            WITH COLLECT(n0)+COLLECT(n1) AS n_list
            UNWIND n_list AS n
            WITH COUNT(DISTINCT n) AS metric
            RETURN metric;
            """
        else:
            query += """
            MATCH (n:Neuron)
            WITH COUNT(n) AS metric
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
            WITH COUNT(DISTINCT cs) AS metric
            RETURN metric;
            """
        else:
            query += """
            MATCH (n:Neuron)-[c:ConnectsTo]->()
            WITH COUNT(c) AS metric
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
            """
        else:
            query += """
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
            MATCH (n0)-[c:ConnectsTo]->(n1)
            MATCH (n:Neuron)-[c]-()
            WITH n, COUNT(DISTINCT c) AS degree
            WHERE degree = 1
            WITH COUNT(DISTINCT n) AS metric
            RETURN metric;
            """
        else:
            query += """
            MATCH (n:Neuron)-[c:ConnectsTo]-()
            WITH n, COUNT(c) AS degree
            WHERE degree = 1
            WITH COUNT(DISTINCT n) AS metric
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
            MATCH (n0)-[c:ConnectsTo]->(n1)
            MATCH (n:Neuron)-[c]-()
            WITH n, COUNT(DISTINCT c) AS degree
            WHERE degree > 1000
            WITH COUNT(DISTINCT n) AS metric
            RETURN metric;
            """
        else:
            query += """
            MATCH (n:Neuron)-[c:ConnectsTo]-()
            WITH n, COUNT(c) AS degree
            WHERE degree > 1000
            WITH COUNT(DISTINCT n) AS metric
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
            MATCH (n0)-[c:ConnectsTo]->(n1)
            MATCH (n:Neuron)-[c]-()
            WITH n, COUNT(DISTINCT c) AS degree
            WITH MAX(degree) AS metric
            RETURN metric;
            """
        else:
            query += """
            MATCH (n:Neuron)-[c:ConnectsTo]-()
            WITH n, COUNT(c) AS degree
            WITH MAX(degree) AS metric
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
            MATCH (n0)-[c:ConnectsTo]->(n1)
            MATCH (n:Neuron)-[c]-()
            WITH n, COUNT(DISTINCT c) AS degree
            WITH AVG(degree) AS metric
            RETURN metric;
            """
        else:
            query += """
            MATCH (n:Neuron)-[c:ConnectsTo]-()
            WITH n, COUNT(c) AS degree
            WITH AVG(degree) AS metric
            RETURN metric;
            """
        return self._compute_metric(query)

    def number_of_orphans(
            self,
            bounding_box:BoundingBox=None) -> int:
        if bounding_box is not None:
            node_query:str = self._spatial_node_query(bounding_box)
            relationship_query:str = self._spatial_relationship_query(bounding_box)
            query:str = """
            CALL algo.unionFind.stream("{}", "{}", {{graph: "cypher"}})
            YIELD nodeId, setId
            WITH setId, COUNT(nodeId) AS order_of_component
            WHERE order_of_component = 1
            WITH COUNT(order_of_component) AS metric
            RETURN metric;
            """.format(
                    node_query,
                    relationship_query)
        else:
            query:str = """
            CALL algo.unionFind.stream("Neuron", "ConnectsTo", {graph: "huge"})
            YIELD nodeId, setId
            WITH setId, COUNT(nodeId) AS order_of_component
            WHERE order_of_component = 1
            WITH COUNT(order_of_component) AS metric
            RETURN metric;
            """
        return self._compute_metric(query)

    def number_of_lone_pairs(
            self,
            bounding_box:BoundingBox=None) -> int:
        if bounding_box is not None:
            node_query:str = self._spatial_node_query(bounding_box)
            relationship_query:str = self._spatial_relationship_query(bounding_box)
            query:str = """
            CALL algo.unionFind.stream("{}", "{}", {{graph: "cypher"}})
            YIELD nodeId, setId
            WITH setId, COUNT(nodeId) AS order_of_component
            WHERE order_of_component = 2
            WITH COUNT(order_of_component) AS metric
            RETURN metric;
            """.format(
                    node_query,
                    relationship_query)
        else:
            query:str = """
            CALL algo.unionFind.stream("Neuron", "ConnectsTo", {graph: "huge"})
            YIELD nodeId, setId
            WITH setId, COUNT(nodeId) AS order_of_component
            WHERE order_of_component = 2
            WITH COUNT(order_of_component) AS metric
            RETURN metric;
            """
        return self._compute_metric(query)    

    def max_strongly_connected_components_order(
            self,
            bounding_box:BoundingBox=None) -> int:
        if bounding_box is not None:
            node_query:str = self._spatial_node_query(bounding_box)
            relationship_query:str = self._spatial_relationship_query(bounding_box)
            query:str = """
            CALL algo.scc("{}", "{}", {{graph: "cypher"}})
            YIELD maxSetSize
            WITH maxSetSize AS metric
            RETURN metric;
            """.format(
                    node_query,
                    relationship_query)
        else:
            query:str = """
            CALL algo.scc("Neuron", "ConnectsTo")
            YIELD maxSetSize
            WITH maxSetSize AS metric
            RETURN metric;
            """
        return self._compute_metric(query)

    def max_weakly_connected_components_order(
            self,
            bounding_box:BoundingBox=None) -> int:
        if bounding_box is not None:
            node_query:str = self._spatial_node_query(bounding_box)
            relationship_query:str = self._spatial_relationship_query(bounding_box)
            query:str = """
            CALL algo.unionFind.stream("{}", "{}", {{graph: "cypher"}})
            YIELD nodeId, setId
            WITH setId, COUNT(nodeId) AS order_of_component
            WITH MAX(order_of_component) AS metric
            RETURN metric;
            """.format(
                    node_query,
                    relationship_query)
        else:
            query:str = """
            CALL algo.unionFind.stream("Neuron", "ConnectsTo", {graph: "huge"})
            YIELD nodeId, setId
            WITH setId, COUNT(nodeId) AS order_of_component
            WITH MAX(order_of_component) AS metric
            RETURN metric;
            """
        return self._compute_metric(query)

    def _spatial_subset(self, bounding_box:BoundingBox) -> str:
        subset:str = """
        MATCH (cs:ConnectionSet)-[:Contains]->(s:Synapse)
        WHERE point({{x:{},y:{},z:{}}}) <= s.location < point({{x:{},y:{},z:{}}})
        WITH DISTINCT cs
        MATCH (n0:Neuron)<-[:From]-(cs)-[:To]->(n1:Neuron)
        """.format(
                bounding_box.x_lower,
                bounding_box.y_lower,
                bounding_box.z_lower,
                bounding_box.x_upper,
                bounding_box.y_upper,
                bounding_box.z_upper)
        return subset

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

