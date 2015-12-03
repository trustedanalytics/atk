<hide>
>>> import trustedanalytics as ta

>>> ta.connect()
-etc-

>>> edge_schema = [('source', ta.int32), ('dest', ta.int32), ('weight', ta.float32)]
>>> edge_rows = [ [1, 3, .5], [1, 4, .6], [1, 5, .7], [2, 5, .1] ]

>>> edge_frame = ta.Frame(ta.UploadRows (edge_rows, edge_schema))
<progress>
>>> edge_rows_predict = [ [1, 3, .5], [1, 4, .6], [1, 5, .7], [2, 5, .1] ]
>>> edge_frame_predict = ta.Frame(ta.UploadRows (edge_rows_predict, edge_schema))
-etc-

</hide>
>>> model = ta.CollaborativeFilteringModel()
<progress>
>>> model.train(edge_frame, 'source', 'dest', 'weight')
<progress>

<skip>
>>> result = model.predict(edge_frame_predict, 'source', 'dest', 'weight')
<progress>
>>> result.inspect()
    [#]  weight  product  rating
    ======================================
    [0]       1        4   0.0485362410545
    [1]       1        5   0.0300528816879
    [2]       2        5  0.00397309847176
    [3]       1        3   0.0404468663037

<progress>
</skip>
<hide>
>>> result = model.predict(edge_frame_predict, 'source', 'dest', 'weight')
<progress>
</hide>