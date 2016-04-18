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
>>> result = graph.export_to_orientdb("plocal:/home/wtaie/graphDBs_home/orientdb-community-2.1.12/databases/OrientDbDocTest","admin","admin",5)
<progress>
>>> result['db_uri']
u'plocal:/home/wtaie/graphDBs_home/orientdb-community-2.1.12/databases/OrientDbDocTest'
>>> result['o_vertices_stats']
 {u'source': 5}
>>> result['o_edge_stats']
 {u'edges': 5}



