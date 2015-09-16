.. index::
    single: clustering coefficient

Clustering Coefficients
=======================

The clustering coefficient of a graph provides a measure of how tightly
clustered an undirected graph is.
Informally, if the edge relation denotes "friendship", the clustering
coefficient of the graph is the probability that two people are friends given
that they share a common friend.

.. index::
    pair: algorithm; clustering coefficients

More formally:

.. math::

    cc(G)  = \frac{ \| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\}, \{v,w \} \in \
    E \} \| }{\| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\} \in E \} \|}


Analogously, the clustering coefficient of a vertex provides a measure of how
tightly clustered that vertex's neighborhood is.
Informally, if the edge relation denotes "friendship", the clustering
coefficient at a vertex :math:`v` is the probability that two acquaintances of
:math:`v` are themselves friends.

More formally:

.. math::

    cc(v)  = \frac{ \| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\}, \{v,w \} \in \
    E \} \| }{\| \{ (u,v,w) \in V^3: \ \{v, u \}, \{v, w\} \in E \} \|}


The toolkit provides the function clustering_coefficient which computes both
local and global clustering coefficients for a given undirected graph.

For more details on the mathematics and applications of clustering
coefficients, see http://en.wikipedia.org/wiki/Clustering_coefficient.

