
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
>>> model = ta.GmmModel()
<progress>
>>> train_output = model.train(frame, ["data"], [1.0], 4)
<progress>
<skip>
>>> train_output
{u'cluster_size': {u'Cluster:0': 4, u'Cluster:1': 5},
 u'gaussians': [[u'mu:[6.79969916638852]',
   u'sigma:List(List(2.2623755196701305))'],
  [u'mu:[1.1984454608177824]', u'sigma:List(List(0.5599200477022921))'],
  [u'mu:[6.6173304476544335]', u'sigma:List(List(2.1848346923369246))']],
 u'weights': [0.2929610525524124, 0.554374326098111, 0.15266462134947675]}
</skip>
>>> predicted_frame = model.predict(frame, ["data"])
<progress>
<skip>
>>> predicted_frame.inspect()
[#]  data  name  predicted_cluster
==================================
[0]   9.0  ij                    0
[1]   2.0  ab                    1
[2]   1.0  cd                    1
[3]   0.0  mn                    1
[4]   1.0  gh                    1
[5]   6.0  op                    0
[6]   5.0  qr                    0
[7]   2.0  kl                    1
[8]   7.0  ef                    0
</skip>
