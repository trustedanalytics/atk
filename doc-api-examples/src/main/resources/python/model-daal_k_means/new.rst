
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[2,"ab"],[1,"cd"],[7,"ef"],[1,"gh"],[9,"ij"],[2,"kl"],[0,"mn"],
...                               [6,"op"],[5,"qr"], [120, "outlier"]],  [("data", ta.float64),("name", str)]))
-etc-

</hide>
Consider the following frame containing two columns.

>>> frame.inspect()
[#]  data   name     cluster
============================
[0]    2.0  ab             1
[1]    1.0  cd             1
[2]    7.0  ef             1
[3]    1.0  gh             1
[4]    9.0  ij             1
[5]    2.0  kl             1
[6]    0.0  mn             1
[7]    6.0  op             1
[8]    5.0  qr             1
[9]  120.0  outlier        0

>>> model = ta.DaalKMeansModel()
<progress>
>>> train_output = model.train(frame, ["data"],  2, max_iterations = 20)
<progress>
<skip>
>>> train_output
{'assignments': Frame  <unnamed>
 row_count = 10
 schema = [data:float64, name:unicode, cluster:int32]
 status = ACTIVE  (last_read_date = 2016-03-14T19:10:31.601000-07:00),
 'centroids': {u'Cluster:1': [120.0], u'Cluster:2': [3.6666666666666665]}}
</skip>
>>> predicted_frame = model.predict(frame, ["data"])
<progress>
>>> predicted_frame.inspect()
[#]  data   name     cluster
============================
[0]    2.0  ab             1
[1]    1.0  cd             1
[2]    7.0  ef             1
[3]    1.0  gh             1
[4]    9.0  ij             1
[5]    2.0  kl             1
[6]    0.0  mn             1
[7]    6.0  op             1
[8]    5.0  qr             1
[9]  120.0  outlier        0
<progress>