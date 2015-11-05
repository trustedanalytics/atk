<skip>
>>> import trustedanalytics as ta

>>> ta.connect()
>>> vertex_schema = [('source', ta.int32), ('label', ta.float32)]
>>> edge_schema = [('source', ta.int32), ('dest', ta.int32), ('weight', ta.int32)]

>>> vertex_rows = [ [1, 1], [2, 1], [3, 5], [4, 5], [5, 5] ]
>>> edge_rows = [ [1, 2, 1], [1, 3, 1], [2, 3, 1], [1, 4, 1], [4, 5, 1] ]
>>> vertex_frame = ta.Frame(ta.UploadRows (vertex_rows, vertex_schema))
>>> edge_frame = ta.Frame(ta.UploadRows (edge_rows, edge_schema))

>>> graph = ta.Graph()

>>> graph.define_vertex_type('source')
>>> graph.vertices['source'].add_vertices(vertex_frame, 'source', 'label')

>>> graph.define_edge_type('edges','source', 'source', directed=False)
>>> graph.edges['edges'].add_edges(edge_frame, 'source', 'dest', ['weight'])
>>> result = graph.graphx_label_propagation()
>>> result['source'].inspect()
>>> graph.edges['edges'].inspect()
>>> graph.vertices['vertices'].inspect()

</skip>
