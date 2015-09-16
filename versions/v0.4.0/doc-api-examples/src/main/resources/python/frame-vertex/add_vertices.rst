Examples
--------
.. only:: html

    .. code::

        >>> graph = ta.Graph()
        >>> graph.define_vertex_type('users')
        >>> graph.vertices['users'].add_vertices(frame, 'user_id', ['user_name', 'age'])

.. only:: latex

    .. code::

        >>> graph = ta.Graph()
        >>> graph.define_vertex_type('users')
        >>> graph.vertices['users'].add_vertices(frame, 'user_id',
        ... ['user_name', 'age'])


