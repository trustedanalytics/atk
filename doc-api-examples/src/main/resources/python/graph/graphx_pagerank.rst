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
>>> result = graph.graphx_pagerank(output_property="PageRank",max_iterations=2,convergence_tolerance=0.001)
<progress>
>>> graph.edges['edges'].inspect()
    [#]  _eid  _src_vid  _dest_vid  _label  weight
    ==============================================
    [0]     6         1          2  edges        1
    [1]     7         1          3  edges        1
    [2]     9         1          4  edges        1
    [3]     8         2          3  edges        1
    [4]    10         4          5  edges        1
>>> result['edge_dictionary']['edges'].row_count
10
>>> result['vertex_dictionary']['source'].row_count
5
