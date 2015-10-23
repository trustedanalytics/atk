
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[2,"ab"],[1,"cd"],[7,"ef"],[1,"gh"],[9,"ij"],[2,"kl"],[0,"mn"],[6,"op"],[5,"qr"]],
...                               [("data", ta.float64),("name", str)]))
-etc-

</hide>
Consider the following frame containing two columns.

>>> frame.inspect()
[#]  data  name
===============
[0]   2.0  ab
[1]   1.0  cd
[2]   7.0  ef
[3]   1.0  gh
[4]   9.0  ij
[5]   2.0  kl
[6]   0.0  mn
[7]   6.0  op
[8]   5.0  qr
>>> model = ta.KMeansModel()
<progress>
>>> train_output = model.train(frame, ["data"], [1], 3)
<progress>
>>> train_output.has_key('within_set_sum_of_squared_error')
True
>>> predicted_frame = model.predict(frame, ["data"])
<progress>
>>> predicted_frame.column_names
[u'data', u'name', u'distance_from_cluster_1', u'distance_from_cluster_2', u'distance_from_cluster_3', u'predicted_cluster']
>>> model.publish()
<progress>