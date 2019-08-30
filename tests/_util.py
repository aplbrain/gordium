from pandas import DataFrame

def get_cc_edgeframe():
    src_key = "presyn_segid"
    tgt_key = "postsyn_segid"
    edges = [
            {src_key: 0, tgt_key: 1},
            {src_key: 1, tgt_key: 2},
            {src_key: 2, tgt_key: 1},
            {src_key: 2, tgt_key: 3},
            {src_key: 4, tgt_key: 5},
            {src_key: 5, tgt_key: 6},
            {src_key: 6, tgt_key: 4}]
    edgeframe = DataFrame(edges)
    return edgeframe

def get_complete_edgeframe(n):
    src_key = "presyn_segid"
    tgt_key = "postsyn_segid"
    edges = list()
    for i in range(n):
        for j in range(i+1, n):
            edges.append({src_key: i, tgt_key: j})
    edgeframe = DataFrame(edges)
    return edgeframe

def get_cc_analytics():
    analytics = DataFrame([{
            "number_of_nodes": 7,
            "number_of_edges": 7,
            "number_of_loops": 0,
            "number_of_leaves": 2,
            "number_of_nodes_with_degree_over_1000": 0,
            "max_degree": 3,
            "mean_degree": 2,
            "number_of_orphans": 0,
            "number_of_lone_pairs": 0,
            "max_strongly_connected_component_order": 3,
            "max_weakly_connected_component_order": 4}])
    return analytics

def get_complete_analytics(n):
    analytics = DataFrame([{
            "number_of_nodes": n,
            "number_of_edges": n*(n-1)/2,
            "number_of_loops": 0,
            "number_of_leaves": 0,
            "number_of_nodes_with_degree_over_1000": n if n > 1001 else 0,
            "max_degree": n-1,
            "mean_degree": n-1,
            "number_of_orphans": 0,
            "number_of_lone_pairs": 1 if n == 2 else 0,
            "max_strongly_connected_component_order": 0,
            "max_weakly_connected_component_order": n}])
    return analytics
