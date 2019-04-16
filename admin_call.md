docker run -it --rm -v "$(pwd)/data":/data -v "$(pwd)/import:/import" --env NEO4J_AUTH=neo4j/neuprint --env NEO4J_dbms_memory_pagecache_size=10G --env NEO4J_dbms_memory_heap_max__size=15G -p 7474:7474 -p 7687:7687 --entrypoint bash neo4j

neo4j-admin import --nodes=/import/neurons.csv --nodes=/import/synapses.csv --relationships=/import/_connects_to.csv --relationships=/import/_from.csv --relationships=/import/_to.csv

docker run --detach --rm -v "$(pwd)/data":/data --env NEO4J_AUTH=neo4j/neuprint --env NEO4J_dbms_memory_pagecache_size=10G --env NEO4J_dbms_memory_heap_max__size=15G -p 7474:7474 -p 7687:7687 neo4j

CREATE INDEX ON :Synapse(location);

MATCH (n0:Neuron)<-[:From]-(s:Synapse)-[:To]->(n1:Neuron) WHERE point({x:180000,y:220000,z:500}) < s.location < point({x:190000,y:230000,z:560}) RETURN (n0)-[:ConnectsTo]->(n1);

MATCH (n0:Neuron)<-[:From]-(s:Synapse)-[:To]->(n1:Neuron) WHERE point({x:180000,y:220000,z:500}) < s.location < point({x:190000,y:230000,z:560}) MATCH (n0)-[c:ConnectsTo]->(n1) RETURN DISTINCT n0, COUNT(DISTINCT c);

