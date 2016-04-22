Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[0,0],[1, 2.5],[2, 5.0],[3, 7.5],[4, 10],[5, 12.5],[6, 13.0],[7, 17.15], [8, 18.5],[9, 23.5]],[("x1", ta.float64),("y", ta.float64)]))
-etc-

</hide>
Consider the following frame containing two columns.

>>> frame.inspect()
[#]  x1   y
===============
[0]  0.0    0.0
[1]  1.0    2.5
[2]  2.0    5.0
[3]  3.0    7.5
[4]  4.0   10.0
[5]  5.0   12.5
[6]  6.0   13.0
[7]  7.0  17.15
[8]  8.0   18.5
[9]  9.0   23.5

>>> model = ta.DaalLinearRegressionModel()
<progress>
>>> train_output = model.train(frame,'y',['x1'])
<progress>
<skip>
>>> train_output
{u'explained_variance': 49.275928030303035,
 u'intercept': -0.03272727272727477,
 u'mean_absolute_error': 0.5299393939393953,
 u'mean_squared_error': 0.6300969696969696,
 u'observation_columns': [u'x1'],
 u'r_2': 0.987374330660537,
 u'root_mean_squared_error': 0.7937864761363534,
 u'value_column': u'y',
 u'weights': [2.443939393939394]}
</skip>
>>> test_output = model.test(frame,'y')
<progress>
<skip>
>>> test_output
{u'explained_variance': 49.275928030303035,
 u'mean_absolute_error': 0.5299393939393953,
 u'mean_squared_error': 0.6300969696969696,
 u'r_2': 0.987374330660537,
 u'root_mean_squared_error': 0.7937864761363534}
</skip>
>>> predicted_frame = model.predict(frame, observation_columns = ["x1"])
<progress>
<skip>
>>> predicted_frame.inspect()
[#]  x1   y      predict_y
=================================
[0]  0.0    0.0  -0.0327272727273
[1]  1.0    2.5     2.41121212121
[2]  2.0    5.0     4.85515151515
[3]  3.0    7.5     7.29909090909
[4]  4.0   10.0     9.74303030303
[5]  5.0   12.5      12.186969697
[6]  6.0   13.0     14.6309090909
[7]  7.0  17.15     17.0748484848
[8]  8.0   18.5     19.5187878788
[9]  9.0   23.5     21.9627272727
</skip>
>>> model.publish()