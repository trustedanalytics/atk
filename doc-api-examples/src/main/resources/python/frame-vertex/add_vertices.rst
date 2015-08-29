Examples
--------
.. only:: html

    .. code::

        >>> graph = atk.Graph()
        >>> graph.define_vertex_type('users')
        >>> graph.vertices['users'].add_vertices(frame, 'user_id', ['user_name', 'age'])

.. only:: latex

    .. code::

        >>> graph = atk.Graph()
        >>> graph.define_vertex_type('users')
        >>> graph.vertices['users'].add_vertices(frame, 'user_id',
        ... ['user_name', 'age'])


