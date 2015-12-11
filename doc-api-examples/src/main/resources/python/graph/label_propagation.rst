<hide>
>>> import trustedanalytics as ta

>>> ta.connect()
-etc-

>>> vertex_schema = [('source', ta.int32), ('label', str), ('waslabeled', ta.int32)]
>>> edge_schema = [('source', ta.int32), ('dest', ta.int32), ('weight', ta.int32)]

>>> vertex_rows = [ [1, "0.1, 0.9", 1], [2, "0.6, 0.4", 1], [3, "0.5, 0.5", 1], [4, "0.7, 0.3", 1], [5, "0.9, 0.1", 1]]
>>> edge_rows = [ [1, 2, 1], [1, 3, 1], [2, 3, 1], [1, 4, 1], [2, 5, 1] ]
>>> vertex_frame = ta.Frame(ta.UploadRows (vertex_rows, vertex_schema))
<progress>
>>> edge_frame = ta.Frame(ta.UploadRows (edge_rows, edge_schema))
<progress>
>>> edge_frame.inspect()
-etc-

</hide>
>>> vertex_frame.inspect()
    [#]  source  label     waslabeled
    =================================
    [0]       1  0.1, 0.9           1
    [1]       2  0.6, 0.4           1
    [2]       3  0.5, 0.5           1
    [3]       4  0.7, 0.3           1
    [4]       5  0.9, 0.1           1

>>> graph = ta.Graph()
>>> graph.define_vertex_type('source')
<progress>
>>> graph.vertices['source'].add_vertices(vertex_frame, 'source', ['label', 'waslabeled'])
<progress>
>>> graph.define_edge_type('edges','source', 'source', directed=False)
<progress>
>>> graph.edges['edges'].add_edges(edge_frame, 'source', 'dest', 'weight')
<progress>
>>> result = graph.label_propagation(prior_property = "label", posterior_property = "label_output", was_labeled_property_name = "waslabeled", alpha=0.2, state_space_size = 2, edge_weight_property = "weight")
<progress>
>>> result_frame = result["vertex_dictionary"]["source"]
>>> result_frame.inspect()
    [#]  _vid  _label  label     label_output  source  waslabeled
    =============================================================
    [0]     5  source  0.9, 0.1  0.9, 0.1           5           1
    [1]     1  source  0.1, 0.9  0.1, 0.9           1           1
    [2]     2  source  0.6, 0.4  0.6, 0.4           2           1
    [3]     3  source  0.5, 0.5  0.5, 0.5           3           1
    [4]     4  source  0.7, 0.3  0.7, 0.3           4           1

>>> vertex_rows = [ [1, "0.1, 0.9", 1], [2, "0.6, 0.4", 1], [3, "", 0], [4, "", 0], [5, "", 0]]
>>> edge_rows = [ [1, 2, 10], [1, 3, 15], [2, 3, 25], [1, 4, 35], [2, 4, 50] ]
>>> vertex_frame = ta.Frame(ta.UploadRows (vertex_rows, vertex_schema))
<progress>
>>> edge_frame = ta.Frame(ta.UploadRows (edge_rows, edge_schema))
<progress>
>>> vertex_frame.inspect()
    [#]  source  label     waslabeled
    =================================
    [0]       1  0.1, 0.9           1
    [1]       2  0.6, 0.4           1
    [2]       3                     0
    [3]       4                     0
    [4]       5                     0


>>> graph = ta.Graph()
>>> graph.define_vertex_type('source')
<progress>
>>> graph.vertices['source'].add_vertices(vertex_frame, 'source', ['label', 'waslabeled'])
<progress>
>>> graph.define_edge_type('edges','source', 'source', directed=False)
<progress>
>>> graph.edges['edges'].add_edges(edge_frame, 'source', 'dest', ['weight'])
<progress>
>>> result = graph.label_propagation(prior_property = "label", posterior_property = "label_output", was_labeled_property_name = "waslabeled", alpha=0.5, state_space_size = 2, edge_weight_property = "weight")
<progress>
>>> result_frame = result["vertex_dictionary"]["source"]
>>> result_frame.inspect()
[#]  _vid  _label  label     label_output                             source
============================================================================
[0]     5  source            0.5, 0.5                                      5
[1]     1  source  0.1, 0.9  0.1, 0.9                                      1
[2]     2  source  0.6, 0.4  0.6, 0.4                                      2
[3]     3  source            0.45625, 0.54375                              3
[4]     4  source            0.44705882352941173, 0.5529411764705883       4
<BLANKLINE>
[#]  waslabeled
===============
[0]           0
[1]           1
[2]           1
[3]           0
[4]           0



