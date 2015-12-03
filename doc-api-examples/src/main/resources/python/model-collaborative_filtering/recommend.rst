<hide>
>>> import trustedanalytics as ta

>>> ta.connect()
-etc-
</hide>

>>> edge_schema = [('source', ta.int32), ('dest', ta.int32), ('weight', ta.float32)]
>>> edge_rows = [ [1, 3, .5], [1, 4, .6], [1, 5, .7], [2, 5, .1] ]

>>> edge_frame = ta.Frame(ta.UploadRows (edge_rows, edge_schema))
<progress>
>>> edge_frame.inspect()
    [#]  source  dest  weight
    =================================
    [0]       1     3             0.5
    [1]       1     4  0.600000023842
    [2]       1     5  0.699999988079
    [3]       2     5   0.10000000149

>>> model = ta.CollaborativeFilteringModel()
<progress>
>>> model.train(edge_frame, 'source', 'dest', 'weight')
<progress>
<skip>
>>> recommendations = model.recommend(1, 3, True)
<progress>
>>> recommendations
[[1.0, 4.0, 0.04853620741168263], [1.0, 3.0, 0.04044684021480899], [1.0, 5.0, 0.030052860705627682]]
>>> recommendations = model.recommend(5, 2, False)
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