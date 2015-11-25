<hide>
>>> import trustedanalytics as ta

>>> ta.connect()
-etc-

>>> vertex_schema = [('source', ta.int32), ('label', ta.float32)]
>>> edge_schema = [('source', ta.int32), ('dest', ta.int32), ('weight', ta.float32)]

>>> vertex_rows = [ [1, .1], [2, .1], [3, .5], [4, .5], [5, .5] ]
>>> edge_rows = [ [1, 3, .5], [1, 4, .6], [1, 5, .7], [2, 5, .1] ]
>>> vertex_frame = ta.Frame(ta.UploadRows (vertex_rows, vertex_schema))
<progress>
>>> edge_frame = ta.Frame(ta.UploadRows (edge_rows, edge_schema))
<progress>
>>> vertex_rows_predict = [ [1, .1], [2, .1], [3, .5], [4, .5], [5, .5] ]
>>> edge_rows_predict = [ [1, 3, .5], [1, 4, .6], [1, 5, .7], [2, 5, .1] ]
>>> vertex_frame_predict = ta.Frame(ta.UploadRows (vertex_rows_predict, vertex_schema))
<progress>
>>> edge_frame_predict = ta.Frame(ta.UploadRows (edge_rows_predict, edge_schema))
-etc-

</hide>
>>> graph = ta.Graph()

>>> graph.define_vertex_type('source')
<progress>
>>> graph.vertices['source'].add_vertices(vertex_frame, 'source', 'label')
<progress>
>>> graph.define_edge_type('edges','source', 'source', directed=False)
<progress>
>>> graph.edges['edges'].add_edges(edge_frame, 'source', 'dest', 'weight')
<progress>
>>> model = ta.CollaborativeFilteringModel()
<progress>
>>> model.train(graph, 'weight')
<progress>

>>> graph_predict = ta.Graph()

>>> graph_predict.define_vertex_type('source')
<progress>
>>> graph_predict.vertices['source'].add_vertices(vertex_frame_predict, 'source', 'label')
<progress>
>>> graph_predict.define_edge_type('edges','source', 'source', directed=False)
<progress>
>>> graph_predict.edges['edges'].add_edges(edge_frame_predict, 'source', 'dest', 'weight')
<progress>
<skip>
>>> result = model.predict(graph_predict)
<progress>
>>> result.inspect()
<progress>
</skip>
<hide>
>>> result = model.predict(graph_predict)
<progress>
</hide>