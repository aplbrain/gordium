from sys import argv

import pandas as pd
import plotly.graph_objs as go
import plotly.offline as py

nj_name = argv[1]
nx_name = argv[2]
mode = argv[3]

nj = pd.read_csv(nj_name)
nx = pd.read_csv(nx_name)

nj_algo_times = nj.max_weakly_connected_component_order_time - nj.index_time
nx_algo_times = nx.max_weakly_connected_component_order_time - nx.convert_time - (nx.two_core_order_time - nx.number_of_lone_pairs_time)

nj_algo_trace = go.Scatter(
    x = nj.number_of_edges,
    y = nj_algo_times,
    mode = 'markers',
    name = 'neo4j'
)

nx_algo_trace = go.Scatter(
    x = nx.number_of_edges,
    y = nx_algo_times,
    mode = 'markers',
    name = 'networkx'
)

data_algo = [nj_algo_trace, nx_algo_trace]

layout_algo = go.Layout(
    xaxis=dict(
        type='log',
        autorange=True,
        title='Number of Edges'
    ),
    yaxis=dict(
        type='log',
        autorange=True,
        title='Time (seconds)'
    ),
    title=dict(
        text='Algorithm Runtimes'
    )
)

fig_algo = go.Figure(data=data_algo, layout=layout_algo)

nj_full_times = nj.end_time - nj.generate_time
nx_full_times = nx.end_time - nx.read_time - (nx.two_core_order_time - nx.number_of_lone_pairs_time)

nj_full_trace = go.Scatter(
    x = nj.number_of_edges,
    y = nj_full_times,
    mode = 'markers',
    name = 'neo4j'
)

nx_full_trace = go.Scatter(
    x = nx.number_of_edges,
    y = nx_full_times,
    mode = 'markers',
    name = 'networkx'
)

data_full = [nj_full_trace, nx_full_trace]

layout_full = go.Layout(
    xaxis=dict(
        type='log',
        autorange=True,
        title='Number of Edges'
    ),
    yaxis=dict(
        type='log',
        autorange=True,
        title='Time (seconds)'
    ),
    title=dict(
        text='Full Processing Runtimes'
    )
)

fig_full = go.Figure(data=data_full, layout=layout_full)

if (mode == 'algo'):
    py.plot(fig_algo)
elif (mode == 'full'):
    py.plot(fig_full)
else:
    raise ValueError('mode must be one of: algo full')

