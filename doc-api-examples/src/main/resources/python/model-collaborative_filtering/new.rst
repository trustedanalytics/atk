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
>>> edge_frame.inspect()
    [#]  source  dest  weight
    =================================
    [0]       1     3             0.5
    [1]       1     4  0.600000023842
    [2]       1     5  0.699999988079
    [3]       2     5   0.10000000149

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
<skip>
>>> model.score(1,5)
<progress>
</skip>
<hide>
>>> x = model.score(1,5)
<progress>
>>> "%.2f" % x
'0.03'
>>> x = model.score(2,5)
<progress>
>>> "%.2f" % x
'0.00'
</hide>