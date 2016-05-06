<hide>
>>> import trustedanalytics as ta

>>> ta.connect()
-etc-

>>> vertex_schema = [('source', ta.int32), ('label', ta.float32)]
>>> edge_schema = [('source', ta.int32), ('dest', ta.int32), ('weight', ta.int32)]

>>> vertex_rows = [ [1, 1], [2, 1], [3, 5], [4, 5], [5, 5] ]
>>> edge_rows = [ [1, 2, 1], [1, 3, 1], [2, 3, 1], [1, 4, 1], [4, 5, 1] ]
>>> vertex_frame = ta.Frame(ta.UploadRows (vertex_rows, vertex_schema))
<progress>
>>> edge_frame = ta.Frame(ta.UploadRows (edge_rows, edge_schema))
-etc-

</hide>
>>> graph = ta.Graph()

>>> graph.define_vertex_type('source')
<progress>
>>> graph.vertices['source'].add_vertices(vertex_frame, 'source', 'label')
<progress>
>>> graph.define_edge_type('edges','source', 'source', directed=False)
<progress>
>>> graph.edges['edges'].add_edges(edge_frame, 'source', 'dest', ['weight'])
<progress>
>>> result = graph.export_to_orientdb("OrientDbDocTest",5)
<progress>
>>> result
    {u'db_uri': u'remote:10.24.141.188:2424/OrientDbDocTest',
    u'exported_edges': {u'edges': {u'exported_count': 5, u'failures_count': 0}},
    u'exported_vertices': {u'source': {u'exported_count': 5,u'failures_count': 0}}}



