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
>>> result = graph.graphx_connected_components()
<progress>
>>> result['source'].inspect()
    [#]  _vid  _label  source  label  connectedComponentId
    ======================================================
    [0]     5  source       5    5.0                     1
    [1]     1  source       1    1.0                     1
    [2]     2  source       2    1.0                     1
    [3]     3  source       3    5.0                     1
    [4]     4  source       4    5.0                     1

>>> graph.edges['edges'].inspect()
    [#]  _eid  _src_vid  _dest_vid  _label  weight
    ==============================================
    [0]     6         1          2  edges        1
    [1]     7         1          3  edges        1
    [2]     9         1          4  edges        1
    [3]     8         2          3  edges        1
    [4]    10         4          5  edges        1
