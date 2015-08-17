.. index::
    single: degree calculation

Degree Calculation
==================

A fundamental quantity in graph analyses is the degree of a vertex:
The degree of a vertex is the number of edges adjacent to it.

For a directed edge relation, a vertex has both an out-degree (the number of
edges leaving the vertex) and an in-degree (the number of edges entering the
vertex).

The toolkit provides a routine ``annotate_degrees`` for calculating the degrees
of vertices. This calculation could be performed with a Gremlin query on
smaller datasets because Gremlin queries cannot be executed on a distributed
scale.
The |PACKAGE| routine ``annotate_degrees`` can be executed at distributed scale.

In the presence of edge weights, vertices can have weighted degrees: The
weighted degree of a vertex is the sum of weights of edges adjacent to it.
Analogously, the weighted in-degree of a vertex is the sum of the weights of
the edges entering it, and the weighted out-degree is the sum
of the weights of the edges leaving the vertex.

The toolkit provides ``annotate_weighted_degrees`` for the distributed
calculation of weighted vertex degrees.

