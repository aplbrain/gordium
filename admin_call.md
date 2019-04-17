docker run -it --rm -v "$(pwd)/data":/data -v "$(pwd)/import:/import" --env NEO4J_AUTH=neo4j/neuprint --env NEO4J_dbms_memory_pagecache_size=10G --env NEO4J_dbms_memory_heap_max__size=15G -p 7474:7474 -p 7687:7687 --entrypoint bash neo4j

neo4j-admin import --nodes=/import/neurons.csv --nodes=/import/synapses.csv --nodes=/import/connection_sets.csv --relationships=/import/_connects_to.csv --relationships=/import/_from.csv --relationships=/import/_to.csv --relationships=/import/_contains.csv

docker run --detach --rm -v "$(pwd)/data":/data -v "$(pwd)/plugins":/plugins --env NEO4J_AUTH=neo4j/neuprint --env NEO4J_dbms_security_procedures_unrestricted=algo.\\\* --env NEO4J_dbms_memory_pagecache_size=10G --env NEO4J_dbms_memory_heap_max__size=15G -p 7474:7474 -p 7687:7687 neo4j

CREATE INDEX ON :Synapse(location);
CALL db.indexes();

MATCH (cs:ConnectionSet)-[:Contains]->(s:Synapse) WHERE point({x:189000,y:227000,z:500}) <= s.location < point({x:190000,y:227300,z:570}) WITH DISTINCT cs RETURN cs;

MATCH (cs:ConnectionSet)-[:Contains]->(s:Synapse) WHERE point({x:180000,y:220000,z:500}) <= s.location < point({x:190000,y:230000,z:560}) WITH DISTINCT cs MATCH (n0:Neuron)<-[:From]-(cs)-[:To]->(n1:Neuron) RETURN DISTINCT (n0)-[:ConnectsTo]->(n1);

MATCH (cs:ConnectionSet)-[:Contains]->(s:Synapse) WHERE point({x:180000,y:220000,z:500}) <= s.location < point({x:190000,y:230000,z:560}) WITH DISTINCT cs MATCH (n0:Neuron)<-[:From]-(cs)-[:To]->(n1:Neuron) MATCH (n0)-[c:ConnectsTo]->(n1) RETURN DISTINCT n0, COUNT(DISTINCT c);

CALL algo.scc("MATCH (n:Neuron)<-[:From|:To]-(cs:ConnectionSet)-[:Contains]->(s:Synapse) WHERE point({x:189000,y:227000,z:550}) <= s.location < point({x:190000,y:227300,z:560}) RETURN id(n) AS id;", "MATCH (cs:ConnectionSet)-[:Contains]->(s:Synapse) WHERE point({x:189000,y:227000,z:550}) <= s.location < point({x:190000,y:227300,z:560}) WITH DISTINCT cs MATCH (n0:Neuron)<-[:From]-(cs)-[:To]->(n1:Neuron) RETURN DISTINCT id(n0) AS source, id(n1) AS target", {graph: "cypher"}) YIELD maxSetSize RETURN maxSetSize;

