Examples
--------
Create a frame and add edges:

.. only:: html

    .. code::

        >>> graph = atk.Graph()
        >>> graph.define_vertex_type('users')
        >>> graph.define_vertex_type('movie')
        >>> graph.define_edge_type('ratings', 'users', 'movies', directed=True)
        >>> graph.add_edges(frame, 'user_id', 'movie_id', ['rating'], create_missing_vertices=True)

.. only:: latex

    .. code::

        >>> graph = atk.Graph()
        >>> graph.define_vertex_type('users')
        >>> graph.define_vertex_type('movie')
        >>> graph.define_edge_type('ratings', 'users', 'movies', directed=True)
        >>> graph.add_edges(frame, 'user_id', 'movie_id', ['rating'],
        ...     create_missing_vertices=True)


