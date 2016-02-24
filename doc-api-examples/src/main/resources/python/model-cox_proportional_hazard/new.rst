
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[2201,28.3],[374,22.7],[1002,35.7],[1205,30.7],[2065,26.5],[6,31.4],[98,21.5],[189,27.1],[2421,27.9]],[("time_column", ta.float64),("covariance_column", ta.float64)]))
-etc-

</hide>
>>> frame.inspect()
[#]  time_column  covariance_column
===================================
[0]       2201.0               28.3
[1]        374.0               22.7
[2]       1002.0               35.7
[3]       1205.0               30.7
[4]       2065.0               26.5
[5]          6.0               31.4
[6]         98.0               21.5
[7]        189.0               27.1
[8]       2421.0               27.9

>>> model = ta.CoxProportionalHazardModel()
<progress>

>>> train_output = model.train(frame, "time_column", "covariance_column", 0.01, 0.03)
<progress>
>>> train_output['beta']
0.029976090327051643
>>> train_output['error']
2.3909672948355803e-05
