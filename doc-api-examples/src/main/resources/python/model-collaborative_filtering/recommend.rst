<hide>
>>> import trustedanalytics as ta

>>> ta.connect()
-etc-
</hide>

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
>>> recommendations = model.recommend(1, 3, True)
<progress>
>>> recommendations
[[1.0, 4.0, 0.04852067923086065], [1.0, 3.0, 0.040433898068959945], [1.0, 5.0, 0.03004276419366994]]
>>> recommendations = model.recommend(3, 2, False)
<progress>
>>> recommendations
[[1.0, 3.0, 0.040433898068959945], [2.0, 3.0, 0.00534539621613791]]
</skip>
<hide>
>>> recommendations = model.recommend(1, 3, True)
<progress>
>>> "%.2f" % recommendations[0][2]
'0.05'
>>> "%.2f" % recommendations[1][2]
'0.04'
>>> "%.2f" % recommendations[2][2]
'0.03'
>>> recommendations = model.recommend(3, 2, False)
<progress>
>>> "%.2f" % recommendations[0][2]
'0.04'
</hide>