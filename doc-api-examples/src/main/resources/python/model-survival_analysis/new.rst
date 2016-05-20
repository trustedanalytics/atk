Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

>>> frame = ta.Frame(ta.UploadRows([[6,31.4,1],[98,21.5,1],[189,27.1,1],
...                                 [374,22.7,1],[1002,35.7,1],[1205,30.7,1],
...                                 [2065,26.5,1],[2201,28.3,1], [2421,27.9,1]],
...                                 [("time", ta.float64),("bmi", ta.float64),("censor", ta.float64)]))
-etc-

</hide>
Consider the following frame containing three columns.

>>> frame.inspect()
[#]  time    bmi   censor
=========================
[0]     6.0  31.4     1.0
[1]    98.0  21.5     1.0
[2]   189.0  27.1     1.0
[3]   374.0  22.7     1.0
[4]  1002.0  35.7     1.0
[5]  1205.0  30.7     1.0
[6]  2065.0  26.5     1.0
[7]  2201.0  28.3     1.0
[8]  2421.0  27.9     1.0

>>> model = ta.CoxPhModel()
<progress>
>>> train_output = model.train(frame,time_column='time',covariate_columns=['bmi'],censor_column='censor',convergence_tolerance=0.01,max_steps=10)
<progress>
>>> train_output
{u'beta': [-0.03351902788328831], u'mean': [27.977777777777778]}
>>> train_output['beta']
[-0.03351902788328831]
>>> predict_output = model.predict(frame)
<progress>
<skip>
>>> predict_output.inspect()
[#]  time    bmi   censor  hazard_ratio
=========================================
[0]     6.0  31.4     1.0  0.891625068026
[1]    98.0  21.5     1.0    1.2425041437
[2]   189.0  27.1     1.0   1.02985936884
[3]   374.0  22.7     1.0    1.1935188738
[4]  1002.0  35.7     1.0  0.771945457787
[5]  1205.0  30.7     1.0  0.912792914749
[6]  2065.0  26.5     1.0   1.05078097618
[7]  2201.0  28.3     1.0  0.989257541146
[8]  2421.0  27.9     1.0   1.00261043677
</skip>