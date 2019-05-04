from dotmotif.executors import Neo4jExecutor

from ._backend import BoundingBox, DataFrame, GraphBackend

class NeuPrintBackend(GraphBackend):

    def __init__(self, edgeframe:DataFrame):
        self._graph = Neo4jExecutor(
                db_bolt_uri="bolt://localhost:7687",
                username="neo4j",
                password="neuprint")
        self._dh = None
        self._scch = None
        self._wcch = None

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
            WHERE n0 = n1
            WITH COUNT(DISTINCT n0) AS metric
            RETURN metric;
            """
        else:
            query += """
            MATCH (n0:Neuron)-[:ConnectsTo]->(n1:Neuron)
            WHERE n0 = n1
            WITH COUNT(DISTINCT n0) AS metric
            RETURN metric;
            """
        return self._compute_metric(query)

    def degree_histogram(
            self,
            bounding_box:BoundingBox=None) -> int:
        if self._dh is None:
            query:str = ""
            if bounding_box is not None:
                query += self._spatial_subset(bounding_box)
                query += " "
                query += """
                MATCH (n:Neuron)-[:From|:To]-(cs)
                WITH n, COUNT(DISTINCT cs) AS degree
                WITH degree, COUNT(DISTINCT n) AS frequency
                RETURN degree, frequency;
                """
            else:
                query += """
                MATCH (n:Neuron)-[c:ConnectsTo]-()
                WITH n, COUNT(c) AS degree
                WITH degree, COUNT(DISTINCT n) AS frequency
                RETURN degree, frequency;
                """
            self._dh = self._graph.run(query).to_data_frame()
            self._dh = self._dh.set_index("degree").frequency
        return self._dh

    def scc_histogram(
            self,
            bounding_box:BoundingBox=None) -> int:
        if self._scch is None:
            if bounding_box is not None:
                node_query:str = self._spatial_node_query(bounding_box)
                relationship_query:str = self._spatial_relationship_query(bounding_box)
                query:str = """
                CALL algo.scc.stream("{}", "{}", {{graph: "cypher"}})
                YIELD nodeId, partition
                WITH partition, COUNT(DISTINCT nodeId) AS cc_order
                WITH cc_order, COUNT(DISTINCT partition) AS frequency
                RETURN cc_order, frequency;
                """.format(
                        node_query,
                        relationship_query)
            else:
                query:str = """
                CALL algo.scc.stream("Neuron", "ConnectsTo")
                YIELD nodeId, partition
                WITH partition, COUNT(DISTINCT nodeId) AS cc_order
                WITH cc_order, COUNT(DISTINCT partition) AS frequency
                RETURN cc_order, frequency;
                """
            self._scch = self._graph.run(query).to_data_frame()
            self._scch = self._scch.set_index("cc_order").frequency
        return self._scch

    def wcc_histogram(
            self,
            bounding_box:BoundingBox=None) -> int:
        if self._wcch is None:
            if bounding_box is not None:
                node_query:str = self._spatial_node_query(bounding_box)
                relationship_query:str = self._spatial_relationship_query(bounding_box)
                query:str = """
                CALL algo.unionFind.stream("{}", "{}", {{graph: "cypher"}})
                YIELD nodeId, setId
                WITH setId, COUNT(DISTINCT nodeId) AS cc_order
                WITH cc_order, COUNT(DISTINCT setId) AS frequency
                RETURN cc_order, frequency;
                """.format(
                        node_query,
                        relationship_query)
            else:
                query:str = """
                CALL algo.unionFind.stream("Neuron", "ConnectsTo")
                YIELD nodeId, setId
                WITH setId, COUNT(DISTINCT nodeId) AS cc_order
                WITH cc_order, COUNT(DISTINCT setId) AS frequency
                RETURN cc_order, frequency;
                """
            self._wcch = self._graph.run(query).to_data_frame()
            self._wcch = self._wcch.set_index("cc_order").frequency
        return self._wcch

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
        RETURN DISTINCT ID(n) AS id;
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
        RETURN DISTINCT ID(n0) AS source, ID(n1) AS target;
        """.format(
                bounding_box.x_lower,
                bounding_box.y_lower,
                bounding_box.z_lower,
                bounding_box.x_upper,
                bounding_box.y_upper,
                bounding_box.z_upper)
        return query

    def _compute_metric(self, query):
        return self._graph.run(query).to_data_frame().metric[0]

