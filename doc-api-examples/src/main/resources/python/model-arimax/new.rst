
Consider the following model trained and tested on the sample data set in *frame* 'frame'.
The frame has five columns where "CO_GT" is the time series value and "C6H6_GT", "PT08_S2_NMHC" and "T" are exogenous inputs.

CO_GT - True hourly averaged concentration CO in mg/m^3
C6H6_GT - True hourly averaged Benzene concentration in microg/m^3
PT08_S2_NMHC - Titania hourly averaged sensor response (nominally NMHC targeted)
T - Temperature in C

Data from Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml]. Irvine, CA: University of California, School of Information and Computer Science.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> schema = [("CO_GT", ta.float64),("C6H6_GT", ta.float64),("PT08_S2_NMHC", ta.float64),("T", ta.float64)]
>>> frame = ta.Frame(ta.UploadRows([[2.6, 11.9, 1046.0, 13.6],
...                                 [2.0, 9.4, 955.0, 13.3],
...                                 [2.2, 9.0, 939.0, 11.9],
...                                 [2.2, 9.2, 948.0, 11.0],
...                                 [1.6, 6.5, 836.0, 11.2],
...                                 [1.2, 4.7, 750.0, 11.2],
...                                 [1.2, 3.6, 690.0, 11.3],
...                                 [1.0, 3.3, 672.0, 10.7],
...                                 [0.9, 2.3, 609.0, 10.7],
...                                 [0.6, 1.7, 561.0, 10.3],
...                                 [-200.0, 1.3, 527.0, 10.1],
...                                 [0.7, 1.1, 512.0, 11.0],
...                                 [0.7, 1.6, 553.0, 10.5],
...                                 [1.1, 3.2, 667.0, 10.2],
...                                 [2.0, 8.0, 900.0, 10.8],
...                                 [2.2, 9.5, 960.0, 10.5],
...                                 [1.7, 6.3, 827.0, 10.8],
...                                 [1.5, 5.0, 762.0, 10.5],
...                                 [1.6, 5.2, 774.0, 9.5],
...                                 [1.9, 7.3, 869.0, 8.3],
...                                 [2.9, 11.5, 1034.0, 8.0],
...                                 [2.2, 8.8, 933.0, 8.3],
...                                 [2.2, 8.3, 912.0, 9.7],
...                                 [2.9, 11.2, 1020.0, 9.8],
...                                 [4.8, 20.8, 1319.0, 10.3],
...                                 [6.9, 27.4, 1488.0, 9.7],
...                                 [6.1, 24.0, 1404.0, 9.6],
...                                 [3.9, 12.8, 1076.0, 9.1],
...                                 [1.5, 4.7, 749.0, 8.2],
...                                 [1.0, 2.6, 629.0, 8.2],
...                                 [1.7, 5.9, 805.0, 8.3],
...                                 [1.9, 6.4, 829.0, 7.7],
...                                 [1.4, 4.1, 718.0, 7.1],
...                                 [0.8, 1.9, 574.0, 7.0],
...                                 [-200.0, 1.1, 506.0, 6.1],
...                                 [0.6, 1.0, 501.0, 6.3],
...                                 [0.8, 1.8, 571.0, 6.8],
...                                 [1.4, 4.4, 730.0, 6.4],
...                                 [4.4, 17.9, 1236.0, 7.3],
...                                 [-200.0, 22.1, 1353.0, 9.2],
...                                 [3.1, 14.0, 1118.0, 13.2],
...                                 [2.7, 11.6, 1037.0, 14.3],
...                                 [2.1, 10.2, 986.0, 15.0],
...                                 [2.5, 11.0, 1016.0, 16.1],
...                                 [2.7, 12.8, 1078.0, 16.3],
...                                 [2.9, 14.2, 1122.0, 15.8],
...                                 [2.8, 12.7, 1073.0, 15.9],
...                                 [2.4, 11.7, 1041.0, 16.9]],
...                                 schema=schema))
-etc-
</hide>

>>> frame.inspect(columns=["CO_GT","C6H6_GT","PT08_S2_NMHC","T"])
[#]  CO_GT  C6H6_GT  PT08_S2_NMHC  T
=======================================
[0]    2.6     11.9        1046.0  13.6
[1]    2.0      9.4         955.0  13.3
[2]    2.2      9.0         939.0  11.9
[3]    2.2      9.2         948.0  11.0
[4]    1.6      6.5         836.0  11.2
[5]    1.2      4.7         750.0  11.2
[6]    1.2      3.6         690.0  11.3
[7]    1.0      3.3         672.0  10.7
[8]    0.9      2.3         609.0  10.7
[9]    0.6      1.7         561.0  10.3

>>> model = ta.ArimaxModel()
<progress>

>>> train_output = model.train(frame, "CO_GT", ["C6H6_GT","PT08_S2_NMHC","T"],  2, 2, 1, 0, True, True)
<progress>

>>> train_output
{u'c': -0.9075493080767927, u'ar': [-0.6876349849133049, -0.33038065385185783], u'ma': [-1.283039752947022], u'xreg': [-1.0326823408073342, 0.08721820267076823, -1.8741776454756058]}

>>> test_frame = ta.Frame(ta.UploadRows([[3.9, 19.3, 1277.0, 15.1],
...                                      [3.7, 18.2, 1246.0, 14.4],
...                                      [6.6, 32.6, 1610.0, 12.9],
...                                      [4.4, 20.1, 1299.0, 12.1],
...                                      [3.5, 14.3, 1127.0, 11.0],
...                                      [5.4, 21.8, 1346.0, 9.7],
...                                      [2.7, 9.6, 964.0, 9.5],
...                                      [1.9, 7.4, 873.0, 9.1],
...                                      [1.6, 5.4, 782.0, 8.8],
...                                      [1.7, 5.4, 783.0, 7.8]],
...                                      schema=schema))
-etc-


>>> predicted_frame = model.predict(test_frame, "CO_GT", ["C6H6_GT","PT08_S2_NMHC","T"])
<progress>

>>> predicted_frame.column_names
[u'CO_GT', u'C6H6_GT', u'PT08_S2_NMHC', u'T', u'predicted_y']

>>> predicted_frame.inspect(columns=("CO_GT","predicted_y"))
[#]  CO_GT  predicted_y
=========================
[0]    3.9  1.47994006475
[1]    3.7  6.77881520875
[2]    6.6  6.16894546356
[3]    4.4  7.45349002663
[4]    3.5  8.85479025637
[5]    5.4  6.58078264909
[6]    2.7  6.26275769839
[7]    1.9  4.71901417682
[8]    1.6  3.77627384099
[9]    1.7  1.91766708341

>>> model.publish()
<progress>

Take the path to the published model and run it in the Scoring Engine:

<skip>
>>> import requests
>>> headers = {'Content-type': 'application/json', 'Accept': 'application/json,text/plain'}
</skip>

Post a request to get the metadata about the model

<skip>
>>> r = requests.get('http://mymodel.demotrustedanalytics.com/v2/metadata')
>>> r.text
u'{"model_details":{"model_type":"ARIMAX Model","model_class":"com.cloudera.sparkts.models.ARIMAXModel","model_reader":"org.trustedanalytics.atk.scoring.models.ARIMAXModelReaderPlugin","custom_values":{}},"input":[{"name":"y","value":"Array[Double]"},{"name":"x_values","value":"Array[Double]"}],"output":[{"name":"y","value":"Array[Double]"},{"name":"x_values","value":"Array[Double]"},{"name":"score","value":"Array[Double]"}]}'
</skip>

The MAX model only supports version 2 of the scoring engine.  In the following example, we are using the MAX model
that was trained and published in the example above.  To keep things simple, we just send the first three rows of
'y' values and the corresponding 'x_values' (visitors, wkends, incidentRate, and seasonality).

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v2/score',json={"records":[{"y":[3.9,3.7,6.6],"x_values":[19.3,18.2,32.6,1277.0,1246.0,1610.0,15.1,14.4,12.9]}]})
</skip>

The 'score' value contains an array of predicted y values.

<skip>
>>> r.text
u'{"data":[{"y":[3.9,3.7,6.6],"x_values":[19.3,18.2,32.6,1277.0,1246.0,1610.0,15.1,14.4,12.9],"score":[1.47994006475, 6.77881520875, 6.16894546356]}]}'
</skip>
