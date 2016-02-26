
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

>>> model = ta.CoxProportionalHazardModel()
<progress>

>>> frame = ta.Frame(ta.UploadRows([[2201,28.3],[374,22.7],[1002,35.7],[1205,30.7],[2065,26.5],[6,31.4],[98,21.5],[189,27.1],[2421,27.9]],[("time_column", ta.float64),("covariate_column", ta.float64)]))
-etc-

</hide>
>>> frame.inspect()
[#]  time_column  covariate_column
==================================
[0]       2201.0              28.3
[1]        374.0              22.7
[2]       1002.0              35.7
[3]       1205.0              30.7
[4]       2065.0              26.5
[5]          6.0              31.4
[6]         98.0              21.5
[7]        189.0              27.1
[8]       2421.0              27.9


>>> train_output = model.train(frame, "time_column", "covariate_column", 0.001, beta=0.03)
<progress>
>>> train_output['beta']
0.029976090327051643
>>> train_output['error']
2.3909672948355803e-05

<hide>
>>> frame = ta.Frame(ta.UploadRows([[2201,28.3,1],[374,22.7,1],[1002,35.7,1],[1205,30.7,1],[2065,26.5,1],[6,31.4,1],[98,21.5,1],[189,27.1,1],[2421,27.9,1]],[("time_column", ta.float64),("covariate_column", ta.float64),("censored_column", ta.int32)]))
-etc-

</hide>
>>> frame.inspect()
[#]  time_column  covariate_column  censored_column
===================================================
[0]       2201.0              28.3                1
[1]        374.0              22.7                1
[2]       1002.0              35.7                1
[3]       1205.0              30.7                1
[4]       2065.0              26.5                1
[5]          6.0              31.4                1
[6]         98.0              21.5                1
[7]        189.0              27.1                1
[8]       2421.0              27.9                1



>>> train_output = model.train(frame, "time_column", "covariate_column", 0.001, "censored_column", beta=0.03)
<progress>
>>> train_output['beta']
0.029976090327051643
>>> train_output['error']
2.3909672948355803e-05

<hide>
>>> frame = ta.Frame(ta.UploadRows([[2201,28.3,1],[374,22.7,1],[1002,35.7,1],[1205,30.7,1],[2065,26.5,0],[6,31.4,0],[98,21.5,0],[189,27.1,0],[2421,27.9,0]],[("time_column", ta.float64),("covariate_column", ta.float64),("censored_column", ta.int32)]))
-etc-

</hide>
>>> frame.inspect()
[#]  time_column  covariate_column  censored_column
===================================================
[0]       2201.0              28.3                1
[1]        374.0              22.7                1
[2]       1002.0              35.7                1
[3]       1205.0              30.7                1
[4]       2065.0              26.5                0
[5]          6.0              31.4                0
[6]         98.0              21.5                0
[7]        189.0              27.1                0
[8]       2421.0              27.9                0





>>> train_output = model.train(frame, "time_column", "covariate_column", 0.001, "censored_column", beta=0.03)
<progress>
>>> train_output['beta']
0.029978135698772283
>>> train_output['error']
2.1864301227716293e-05