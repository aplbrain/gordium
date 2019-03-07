# a library for graph partitioning strategies
from collections import namedtuple
import networkx as nx

BoundingBox = namedtuple("BoundingBox", (
        "x_min",
        "x_max",
        "y_min",
        "y_max",
        "z_min",
        "z_max"
))

def partition_networkx(graph: nx.Graph, box: BoundingBox) -> nx.Graph:
    """
    Args:
        graph (nx.Graph): a spatially embedded graph whose edges
                          contain attributes "x", "y", "z" that
                          correspond to positions in 3d space
        box (BoundingBox): an object containing the 6 fields that
                           correspond to minimum and maximum values
                           of the attributes "x", "y", "z" fields,
                           where the minimum value is inclusive and
                           the maximum value is exclusive
    Returns:
        subgraph (nx.Graph): a subgraph of the original graph
                             whose edge positions fall strictly
                             within the bounds of the box
    """
    pass

def partition_neo4j(graph: object, box: BoundingBox) -> object:
    pass

