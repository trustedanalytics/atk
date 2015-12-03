<hide>
>>> import trustedanalytics as ta

>>> ta.connect()
-etc-

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

</hide>
>>> model = ta.CollaborativeFilteringModel()
<progress>
>>> model.train(edge_frame, 'source', 'dest', 'weight')
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