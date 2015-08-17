Examples
--------
.. code::

    >>> import trustedanalytics as ta
    >>> ta.connect()
    >>> dataset = r"datasets/kclique_edges.csv"
    >>> schema = [("source", int64), ("target", int64)]
    >>> csvfile = ta.CsvFile(dataset, schema)
    >>> my_frame = ta.Frame(csvfile)

    >>> my_graph = ta.Graph())
    >>> my_graph.name = "mygraph"
    >>> source_vertex_type = my_graph.define_vertex_type("source")
    >>> target_vertex_type = my_graph.define_vertex_type("target")
    >>> direction_edge_type = my_graph.define_edge_type("direction",
    ... "source", "target", directed=True)

    >>> my_graph.vertices['source'].add_vertices(my_frame, 'source')
    >>> my_graph.vertices['target'].add_vertices(my_frame, 'target')
    >>> my_graph.edges['direction'].add_edges(my_frame, 'source', 'target',
    ... is_directed=True)
    >>> my_titan_graph = my_graph.export_to_titan("mytitangraph"))
    >>> my_titan_graph.ml.kclique_percolation(cliqueSize = 3,
    ... communityPropertyDefaultLabel = "Community")
