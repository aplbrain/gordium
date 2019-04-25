from sys import argv

import pandas as pd
import plotly.offline as py

modes = [
        'absolute',
        'iterative',
        'relative',
        'feature',
]

time_variables = [
        'start_time',
        'generate_time',
        'convert_time',
        'import_time',
        'db_up_time',
        'index_time',
        'number_of_nodes_time',
        'number_of_edges_time',
        'number_of_leaves_time',
        'number_of_nodes_with_degree_over_1000_time', 
        'max_degree_time',
        'mean_degree_time',
        'number_of_orphans_time',
        'number_of_lone_pairs_time',
        'max_strongly_connected_component_order_time',
        'max_weakly_connected_component_order_time',
        'db_down_time',
        'end_time', 
]

feat_variables = [
		'number_of_nodes',
		'number_of_edges',
		'number_of_leaves',
		'number_of_nodes_with_degree_over_1000',
		'max_degree',
		'mean_degree',
		'number_of_orphans',
		'number_of_lone_pairs',
		'max_strongly_connected_component_order',
		'max_weakly_connected_component_order',
]

def get_fig(df, mode):
    fig = dict()
    fig['layout'] = dict()
    fig['layout']['height'] = 512
    fig['layout']['width'] = 1024
    fig['layout']['xaxis'] = {'title': 'Number of Edges', 'type': 'log'}
    if mode == 'absolute':
        fig['data'] = [
                {
                        'x': df.number_of_edges,
                        'y': df[time_variable],
                        'name': time_variable, 'mode': 'markers',
                } for time_variable in time_variables
        ]
        fig['layout']['title'] = {'text': 'Absolute Time in Script'}
        fig['layout']['yaxis'] = {'title': 'Time (seconds)', 'type': 'log'}
    elif mode == 'iterative':
        fig['data'] = [
                {
                        'x': df.number_of_edges,
                        'y': df[time_variable]-df['start_time'],
                        'name': time_variable, 'mode': 'markers',
                } for time_variable in time_variables
        ]
        fig['layout']['title'] = {'text': 'Absolute Time in Iteration'}
        fig['layout']['yaxis'] = {'title': 'Time (seconds)', 'type': 'log'}
    elif mode == 'relative':
        fig['data'] = [
                {
                        'x': df.number_of_edges,
                        'y': df[time_variables[t_ix]]-df[time_variables[t_ix-1]],
                        'name': time_variables[t_ix], 'mode': 'markers',
                } for t_ix in range(1, len(time_variables))
        ]
        fig['layout']['title'] = {'text': 'Relative Time in Iteration'}
        fig['layout']['yaxis'] = {'title': 'Time (seconds)', 'type': 'log'}
    elif mode == 'feature':
        fig['data'] = [
                {
                        'x': df.number_of_edges,
                        'y': df[feat_variable],
                        'name': feat_variable, 'mode': 'markers',
                } for feat_variable in feat_variables
        ]
        fig['layout']['title'] = {'text': 'Feature Values'}
        fig['layout']['yaxis'] = {'title': 'Feature Value', 'type': 'log'}
    return fig

if __name__ == '__main__':
    data_file = argv[1]
    mode = argv[2]
    if mode not in modes:
        raise ValueError('mode must be one of: {}'.format(' '.join(modes)))
    df = pd.read_csv(data_file)
    fig = get_fig(df, mode)
    py.plot(fig)

