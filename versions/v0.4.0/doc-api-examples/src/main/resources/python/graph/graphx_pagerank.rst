Examples
--------
.. only:: html

    .. code::

        >>> graph = ta.Graph()
        >>> graph.define_vertex_type("node")
        >>> graph.vertices["node"].add_vertices(frame,"follows")
        >>> graph.vertices["node"].add_vertices(frame,"followed")
        >>> graph.define_edge_type("e1","node","node",directed=True)
        >>> graph.edges["e1"].add_edges(frame,"follows","followed")
        >>> result = graph.graphx_pagerank(output_property="PageRank",max_iterations=2,convergence_tolerance=0.001)
        >>> vertex_dict = result['vertex_dictionary']
        >>> edge_dict = result['edge_dictionary']

.. only:: latex

    .. code::

        >>> graph = ta.Graph()
        >>> graph.define_vertex_type("node")
        >>> graph.vertices["node"].add_vertices(frame,"follows")
        >>> graph.vertices["node"].add_vertices(frame,"followed")
        >>> graph.define_edge_type("e1","node","node",directed=True)
        >>> graph.edges["e1"].add_edges(frame,"follows","followed")
        >>> result = graph.graphx_pagerank(output_property="PageRank",max_iterations=2,convergence_tolerance=0.001)
        >>> vertex_dict = result['vertex_dictionary']
        >>> edge_dict = result['edge_dictionary']
