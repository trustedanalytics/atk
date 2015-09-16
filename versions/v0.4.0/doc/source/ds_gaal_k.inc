.. index::
    single: k-clique percolation
    single: community detection

K-Clique Percolation
====================

.. _ds_mlal_K-Clique Percolation Algorithm:

------------------------------------------------------------
Community Detection Using the K-Clique Percolation Algorithm
------------------------------------------------------------

Overview
========
Modeling data as a graph captures relations – friendship ties between social
network users or chemical interactions between proteins.
Analyzing the community structure of the graph reveals collections of entities
that are more likely to interact amongst each
other – for example, a community of friends in a social network or the
“community” of highly interacting proteins of a cellular process.

The |PACKAGE| version |version| provides community detection using the k-Clique
percolation method first proposed by Palla et. al. [K1]_ that has been widely
used in many contexts.
Other community detection algorithms may be offered in future releases of the
|PACKAGE|.

.. index::
    pair: algorithm; k-clique percolation

K-Clique Percolation
====================
K-clique percolation is one of many different methods for detecting community
structure in graphs.
Here we provide mathematical background on how communities are defined in the
context of the k-clique percolation algorithm.

A k-clique is with :math:`k` vertices in which all
:math:`\left( \frac {k}{2} \right)` possible edges are present.
A k-clique is certainly a community in the sense that its nodes are all
connected, but, it is too restrictive for most purposes,
since it is natural that a few members of the community may not interact.
The k-clique percolation method forms communities by taking unions of k-cliques
that overlap in :math:`k - 1` vertices.

K-Clique Example:
-----------------
In the graph below, the 3-clique communities are {1, 2, 3, 4} and {4, 5, 6, 7,
8}. Note that the nodes 9, 10, 11, 12 belong to no community at all, whereas 4
belongs to two distinct (but overlapping) communities.

.. image:: ds_mlal_a1.png

Distributed Implementation of K-Clique Community Detection
==========================================================

The implementation of k-clique community detection in |PACKAGE| is a fully
distributed Apache Spark based implementation that follows the map-reduce
algorithm proposed in Varamesh et. al. [K2]_ .

It has the following steps:

1.  A k-clique enumeration method is used to enumerate all k-cliques.
#.  k-cliques are used to build  a “clique graph” by declaring each k-clique to
    be a vertex in a new graph and placing edges between k-cliques that share
    k-1 vertices in the base graph.
#.  A :term:`connected component` analysis is performed on the clique graph.
    Connected components of the clique graph correspond to k-clique communities
    in the base graph.
#.  The connected components information for the clique graph is projected back
    down to the base graph, providing each vertex with the set of k-clique
    communities to which it belongs.

See :ref:`API <api_kmeans>` for implementation details.


