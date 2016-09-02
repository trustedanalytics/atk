
Consider the following model trained and tested on the sample data set in *frame* 'frame'.
The frame has five columns where "y" is the time series value and "vistors", "wkends",
"incidentRate", and "seasonality" are exogenous inputs.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> schema = [("y", ta.float64),("visitors", ta.float64),("wkends", ta.float64),("seasonality", ta.float64),
("incidentRate", ta.float64),("holidayFlag", ta.float64),("postHolidayFlag", ta.float64),("min_temp", ta.float64)]
>>> frame = ta.Frame(ta.UploadRows([[93.0, 416.0, 0.0, 0.006103106, 28.0, 0.0, 0.0, 55.0],
...                                 [82.0, 393.0, 0.0, 0.005381233, 28.0, 0.0, 0.0, 57.0],
...                                 [109.0, 444.0, 0.0, 0.007153103, 28.0, 0.0, 0.0, 53.0],
...                                 [110.0, 445.0, 0.0, 0.007218727, 28.0, 0.0, 0.0, 55.0],
...                                 [109.0, 426.0, 1.0, 0.007153103, 28.0, 0.0, 0.0, 57.0],
...                                 [84.0, 435.0, 1.0, 0.005512483, 28.0, 0.0, 0.0, 50.0],
...                                 [100.0, 471.0, 0.0, 0.006562479, 29.0, 0.0, 0.0, 50.0],
...                                 [91.0, 397.0, 0.0, 0.005971856, 29.0, 0.0, 0.0, 53.0],
...                                 [119.0, 454.0, 0.0, 0.007809351, 29.0, 0.0, 0.0, 51.0],
...                                 [78.0, 416.0, 0.0, 0.005118734, 29.0, 0.0, 0.0, 55.0],
...                                 [99.0, 424.0, 0.0, 0.006496855, 29.0, 0.0, 0.0, 48.0],
...                                 [92.0, 395.0, 1.0, 0.006037481, 29.0, 0.0, 0.0, 46.0],
...                                 [76.0, 401.0, 1.0, 0.004987484, 29.0, 0.0, 0.0, 42.0],
...                                 [99.0, 471.0, 0.0, 0.006496855, 21.0, 0.0, 0.0, 41.0],
...                                 [84.0, 400.0, 0.0, 0.005512483, 21.0, 0.0, 0.0, 48.0],
...                                 [103.0, 418.0, 0.0, 0.006759354, 21.0, 0.0, 0.0, 48.0],
...                                 [107.0, 476.0, 0.0, 0.007021853, 21.0, 0.0, 0.0, 55.0],
...                                 [106.0, 436.0, 0.0, 0.006956228, 21.0, 0.0, 0.0, 59.0],
...                                 [106.0, 442.0, 1.0, 0.006956228, 21.0, 0.0, 0.0, 57.0],
...                                 [89.0, 472.0, 1.0, 0.005840607, 21.0, 0.0, 0.0, 55.0]],
...                                 schema=schema))
-etc-
</hide>

>>> frame.inspect(columns=["y","visitors","wkends","seasonality","incidentRate"])
[#]  y      visitors  wkends  seasonality  incidentRate
=======================================================
[0]   93.0     416.0     0.0  0.006103106          28.0
[1]   82.0     393.0     0.0  0.005381233          28.0
[2]  109.0     444.0     0.0  0.007153103          28.0
[3]  110.0     445.0     0.0  0.007218727          28.0
[4]  109.0     426.0     1.0  0.007153103          28.0
[5]   84.0     435.0     1.0  0.005512483          28.0
[6]  100.0     471.0     0.0  0.006562479          29.0
[7]   91.0     397.0     0.0  0.005971856          29.0
[8]  119.0     454.0     0.0  0.007809351          29.0
[9]   78.0     416.0     0.0  0.005118734          29.0

>>> model = ta.ArimaxModel()
<progress>

>>> train_output = model.train(frame, "y", ["visitors", "wkends", "seasonality", "incidentRate"], 2, 1, 2, 1, False, False)
<progress>

>>> train_output
{u'ar': [-2.1625834191902604],
 u'c': 64.76420579028625,
 u'ma': [2.327546112994998],
 u'xreg': [0.27254631585259576,
  -6.2563111257910915,
  3.181119213309016,
  1678.7804646710351]}

>>> test_frame = ta.Frame(ta.UploadRows([[100.0, 465.0, 1.0, 0.006562479, 24.0, 1.0, 0.0, 51.0],
...                                  [98.0, 453.0, 1.0, 0.00643123, 24.0, 0.0, 1.0, 54.0],
...                                  [102.0, 472.0, 0.0, 0.006693729, 25.0, 0.0, 0.0, 49.0],
...                                  [98.0, 454.0, 0.0, 0.00643123, 25.0, 0.0, 0.0, 46.0],
...                                  [112.0, 432.0, 0.0, 0.007349977, 25.0, 0.0, 0.0, 42.0],
...                                  [99.0, 431.0, 0.0, 0.006496855, 25.0, 0.0, 0.0, 41.0],
...                                  [99.0, 475.0, 0.0, 0.006496855, 25.0, 0.0, 0.0, 45.0],
...                                  [87.0, 393.0, 1.0, 0.005709357, 25.0, 0.0, 0.0, 46.0],
...                                  [103.0, 437.0, 1.0, 0.006759354, 25.0, 0.0, 0.0, 48.0],
...                                  [115.0, 537.0, 0.0, 0.007546851, 23.0, 0.0, 0.0, 41.0]],
...                                  schema=schema))



>>> predicted_frame = model.predict(test_frame, "y", ["visitors", "wkends", "seasonality", "incidentRate"])
<progress>

>>> predicted_frame.column_names
[u'y', u'visitors', u'wkends', u'seasonality', u'incidentRate', u'holidayFlag', u'postHolidayFlag', u'min_temp', u'predicted_y']

>>> predicted_frame.inspect(columns=("y","predicted_y"))
[#]  y      predicted_y
=========================
[0]  100.0  104.813706372
[1]   98.0  104.126348745
[2]  102.0  102.225824121
[3]   98.0  102.499768211
[4]  112.0   102.05793437
[5]   99.0  102.208895499
[6]   99.0  102.087246933
[7]   87.0  102.144009625
[8]  103.0  102.107219659
[9]  115.0  102.126621339

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

The ARIMAX model only supports version 2 of the scoring engine.  In the following example, we are using the ARIMAX model
that was trained and published in the example above.  To keep things simple, we just send the first three rows of
'y' values and the corresponding 'x_values' (visitors, wkends, incidentRate, and seasonality).

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v2/score',json={"records":[{"y":[100.0,98.0,102.0],"x_values":[465.0,453.0,472.0,1.0,1.0,1.0,0.006562479,0.00643123,0.006693729,24.0,24.0,25.0]}]})
</skip>

The 'score' value contains an array of predicted y values.

<skip>
>>> r.text
u'{"data":[{"y":[100.0,98.0,102.0],"x_values":[465.0,453.0,472.0,1.0,1.0,1.0,0.006562479,0.00643123,0.006693729,24.0,24.0,25.0],"score":[104.813706372, 104.126348745, 102.225824121]}]}'
</skip>
