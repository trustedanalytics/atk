<hide>
>>> import trustedanalytics as ta

>>> ta.connect()
-etc-

>>> vertex_schema = [('source', ta.int32), ('label', str)]
>>> edge_schema = [('source', ta.int32), ('dest', ta.int32), ('weight', ta.int32)]

>>> vertex_rows = [ [1, "0.1, 0.2"], [2, "0.3, 0.4"], [3, "0.5, 0.6"], [4, "0.7, 0.8"], [5, "0.9, 0.1"] ]
>>> edge_rows = [ [1, 2, 1], [1, 3, 1], [2, 3, 1], [1, 4, 1], [4, 5, 1] ]
>>> vertex_frame = ta.Frame(ta.UploadRows (vertex_rows, vertex_schema))
<progress>
>>> edge_frame = ta.Frame(ta.UploadRows (edge_rows, edge_schema))
<progress>
>>> edge_frame.inspect()
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
>>> result = graph.loopy_belief_propagation(prior_property = "label", posterior_property = "label_output", state_space_size = 2)
<progress>
>>> result_frame = result["vertex_dictionary"]["source"]

>>> result_frame.inspect()
    [#]  _vid  _label  source  label     label_output
    ============================================================================
    [0]     5  source       5  0.9, 0.1  0.7948407335482741, 0.20515926645172589
    [1]     1  source       1  0.1, 0.2  0.3720286722960857, 0.6279713277039142
    [2]     2  source       2  0.3, 0.4  0.348500085203553, 0.651499914796447
    [3]     3  source       3  0.5, 0.6  0.35295711467274254, 0.6470428853272574
    [4]     4  source       4  0.7, 0.8  0.5631032971375366, 0.4368967028624634


