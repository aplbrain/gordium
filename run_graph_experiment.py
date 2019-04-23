from collections import defaultdict
from math import sqrt
from sys import argv
from subprocess import run
from time import process_time, sleep

from dask.array.random import seed
from pandas import DataFrame

from generate import generate
from gordium import Gordium
from prepare_admin_import import prepare_admin_import

seed(0)

if __name__ == '__main__':
    EDGE_DENSITY = 0.01
    start_time = process_time()
    records = list()
    orders_of_magnitude = range(1, 4)
    for oom in orders_of_magnitude:
        record = dict()
        record['start_time'] = process_time()
        n_edges_desired = 10**oom
        n_nodes = int(1.1*sqrt(n_edges_desired/EDGE_DENSITY))
        edgeframe = generate(n_nodes, edge_density=EDGE_DENSITY)
        edgeframe.to_csv('/store/random/*.csv')
        record['generate_time'] = process_time()
        prepare_admin_import(edgeframe, '/store/import')
        edgeframe = None
        record['convert_time'] = process_time()
        run('docker run -it --rm -v /store/data:/data -v /store/import:/import -v /home/ec2-user/gordium/scripts:/scripts --env NEO4J_AUTH=neo4j/neuprint --env NEO4J_dbms_memory_pagecache_size=50G --env NEO4J_dbms_memory_heap_max__size=750G -p 7474:7474 -p 7687:7687 --entrypoint /scripts/import_command.sh neo4j'.split())
        record['import_time'] = process_time()
        run('docker run --detach --rm -v /store/data:/data -v /home/ec2-user/gordium/plugins:/plugins -v /home/ec2-user/gordium/scripts:/scripts --env NEO4J_AUTH=neo4j/neuprint --env NEO4J_dbms_security_procedures_unrestricted=algo.\* --env NEO4J_dbms_memory_pagecache_size=50G --env NEO4J_dbms_memory_heap_max__size=750G -p 7474:7474 -p 7687:7687 --name neuprint-db neo4j'.split())
        sleep(20)
        record['db_up_time'] = process_time()
        run('docker exec -it neuprint-db /scripts/index_command.sh'.split())
        record['index_time'] = process_time()
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
            record['{}_time'.format(fn.__name__)] = process_time()
        run('docker kill neuprint-db'.split())
        record['db_down_time'] = process_time()
        run('docker run -it --rm -v /store/data:/data -v /store/import:/import -v /home/ec2-user/gordium/scripts:/scripts --entrypoint /scripts/clear_command.sh neo4j'.split())
        record['end_time'] = process_time()
        records.append(record)
        records_df = DataFrame(records)
        records_df.to_csv('graph_experiment_benchmarks.csv', index=False)

