from collections import defaultdict
from math import sqrt
from sys import argv
from subprocess import run
from time import perf_counter, sleep

from dask.array.random import seed
import dask.dataframe as dd
from networkx import DiGraph, from_pandas_edgelist
from pandas import DataFrame

from generate import generate
from gordium import Gordium
from prepare_admin_import import prepare_admin_import
from table import functions as table_fns

seed(0)

if __name__ == '__main__':
    EDGE_DENSITY = 0.01
    start_time = perf_counter()
    neo4j_records = list()
    networkx_records = list()
    orders_of_magnitude = [9] # range(1, 10)
    for oom in orders_of_magnitude:
        record = dict()
        record['start_time'] = perf_counter()
        n_edges_desired = 500e6 # 10**oom
        n_nodes = int(1.1*sqrt(n_edges_desired/EDGE_DENSITY))
        edgeframe = generate(n_nodes, edge_density=EDGE_DENSITY)
        edgeframe.to_csv('/store/random/*.csv', index=False)
        if oom <= 7:
            edgeframe.to_csv('/home/ec2-user/gordium/edgeframe_{}/*.csv'.format(oom), index=False)
        record['generate_time'] = perf_counter()
        edgeframe = None
        # run for Neo4j
        edgeframe = dd.read_csv('/store/random/*.csv')
        record['read_time'] = perf_counter()
        prepare_admin_import(edgeframe, '/store/import')
        edgeframe = None
        record['convert_time'] = perf_counter()
        run('docker run -it --rm -v /store/data:/data -v /store/import:/import -v /home/ec2-user/gordium/scripts:/scripts --env NEO4J_AUTH=neo4j/neuprint --env NEO4J_dbms_memory_pagecache_size=50G --env NEO4J_dbms_memory_heap_max__size=750G -p 7474:7474 -p 7687:7687 --entrypoint /scripts/import_command.sh neo4j'.split())
        record['import_time'] = perf_counter()
        run('docker run --detach --rm -v /store/data:/data -v /home/ec2-user/gordium/plugins:/plugins -v /home/ec2-user/gordium/scripts:/scripts --env NEO4J_AUTH=neo4j/neuprint --env NEO4J_dbms_security_procedures_unrestricted=algo.\* --env NEO4J_dbms_memory_pagecache_size=50G --env NEO4J_dbms_memory_heap_max__size=750G -p 7474:7474 -p 7687:7687 --name neuprint-db neo4j'.split())
        sleep(20)
        record['db_up_time'] = perf_counter()
        run('docker exec -it neuprint-db /scripts/index_command.sh'.split())
        record['index_time'] = perf_counter()
        graph = Gordium(db_bolt_uri='bolt://localhost:7687')
        # so I actually needed to do this
        # because the connection pool seemed to get sour
        # but running ONE query (that will fail) seems to reset it
        try:
            temp = graph.number_of_nodes()
        except:
            pass
        for fn in graph.fns:
            record[fn.__name__] = fn()
            record['{}_time'.format(fn.__name__)] = perf_counter()
        run('docker kill neuprint-db'.split())
        record['db_down_time'] = perf_counter()
        # run('docker run -it --rm -v /store/data:/data -v /store/import:/import -v /home/ec2-user/gordium/scripts:/scripts --entrypoint /scripts/clear_command.sh neo4j'.split())
        graph = None
        record['end_time'] = perf_counter()
        neo4j_records.append(record)
        neo4j_records_df = DataFrame(neo4j_records)
        neo4j_records_df.to_csv('graph_experiment_benchmarks_neo4j_500m.csv', index=False)
        # run for NetworkX
        record = dict()
        record['start_time'] = perf_counter()
        edgeframe = dd.read_csv('/store/random/*.csv').compute()
        record['read_time'] = perf_counter()
        graph = from_pandas_edgelist(
                edgeframe,
                source='presyn_segid',
                target='postsyn_segid',
                edge_attr=True,
                create_using=DiGraph)
        edgeframe = None
        record['convert_time'] = perf_counter()
        for fn in table_fns:
            record[fn.__name__] = fn(graph)
            record['{}_time'.format(fn.__name__)] = perf_counter()
        graph = None
        record['end_time'] = perf_counter()
        networkx_records.append(record)
        networkx_records_df = DataFrame(networkx_records)
        networkx_records_df.to_csv('graph_experiment_benchmarks_networkx_500m.csv', index=False)

