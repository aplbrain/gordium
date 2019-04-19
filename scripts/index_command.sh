#!/bin/bash

echo "CREATE INDEX ON :Synapse(location);" | cypher-shell -u neo4j -p neuprint
echo "CALL db.awaitIndex(\":Synapse(location)\");" | cypher-shell -u neo4j -p neuprint

