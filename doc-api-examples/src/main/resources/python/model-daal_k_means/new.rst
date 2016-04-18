
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
[#]  data   name
===================
[0]    2.0  ab
[1]    1.0  cd
[2]    7.0  ef
[3]    1.0  gh
[4]    9.0  ij
[5]    2.0  kl
[6]    0.0  mn
[7]    6.0  op
[8]    5.0  qr
[9]  120.0  outlier

>>> model = ta.DaalKMeansModel()
<progress>
>>> train_output = model.train(frame, ["data"],  k=2, max_iterations = 20)
<progress>
<skip>
>>> train_output
{u'centroids': {u'Cluster:0': [120.0], u'Cluster:1': [3.6666666666666665]},
 u'cluster_sizes': {u'Cluster:0': 1, u'Cluster:1': 9}}
</skip>
>>> predicted_frame = model.predict(frame, ["data"])
<progress>
>>> predicted_frame.inspect()
<skip>
[#]  data   name     distance_from_cluster_0  distance_from_cluster_1  cluster
==============================================================================
[0]    2.0  ab                       13924.0            2.77777777778        1
[1]    1.0  cd                       14161.0            7.11111111111        1
[2]    7.0  ef                       12769.0            11.1111111111        1
[3]    1.0  gh                       14161.0            7.11111111111        1
[4]    9.0  ij                       12321.0            28.4444444444        1
[5]    2.0  kl                       13924.0            2.77777777778        1
[6]    0.0  mn                       14400.0            13.4444444444        1
[7]    6.0  op                       12996.0            5.44444444444        1
[8]    5.0  qr                       13225.0            1.77777777778        1
[9]  120.0  outlier                      0.0            13533.4444444        0
</skip>