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
    [0]     5  source       5  0.9, 0.1  0.872280226672143, 0.12771977332785708
    [1]     1  source       1  0.1, 0.2  0.3035580315026917, 0.6964419684973083
    [2]     2  source       2  0.3, 0.4  0.31594418001501606, 0.6840558199849839
    [3]     3  source       3  0.5, 0.6  0.32734422879825076, 0.6726557712017492
    [4]     4  source       4  0.7, 0.8  0.5409967503196225, 0.4590032496803774



