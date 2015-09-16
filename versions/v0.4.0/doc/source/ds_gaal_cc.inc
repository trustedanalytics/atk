.. index::
    single: CC
    single: connected components
    single: subgraph
    single: message passing

Connected Components (CC)
=========================

Connected components are disjoint subgraphs in which all vertices are
connected to all other vertices in the same component via paths, but not
connected via paths to vertices in any other component.
The connected components algorithm uses message passing along a specified edge
type to find all of the connected components of a graph and label each edge
with the identity of the component to which it belongs.
The algorithm is specific to an edge type, hence in graphs with several
different types of edges, there may be multiple, overlapping sets of connected
components.

.. index::
    pair: algorithm; connected components

The algorithm works by assigning each vertex a unique numerical index and
passing messages between neighbors.
Vertices pass their indices back and forth with their neighbors and update
their own index as the minimum of their current index and all other indices
received.
This algorithm continues until there is no change in any of the vertex
indices.
At the end of the alorithm, the unique levels of the indices denote the
distinct connected components.
The complexity of the algorithm is proportional to the diameter of the graph.
