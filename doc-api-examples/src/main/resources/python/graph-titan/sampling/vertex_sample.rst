Examples
--------
Assume a set of rules created on a Frame that specifies 'user' and 'product'
vertices as well as an edge rule.
The Graph created from this data can be vertex sampled to obtain a vertex
induced subgraph:

.. code::

    >>> my_graph = atk.TitanGraph([user_vertex_rule, product_vertex_rule, edge_rule])
    >>> my_subgraph = my_graph.sampling.vertex_sample(1000, 'uniform')
