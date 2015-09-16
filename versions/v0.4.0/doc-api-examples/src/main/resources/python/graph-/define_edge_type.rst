Examples
--------
.. code::

    >>> graph = ta.Graph()
    >>> graph.define_vertex_type('users')
    >>> graph.define_vertex_type('movie')
    >>> graph.define_edge_type('ratings', 'users', 'movie', directed=True)
