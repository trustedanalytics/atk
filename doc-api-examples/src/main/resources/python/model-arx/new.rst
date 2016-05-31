
Consider the following model trained and tested on the sample data set in *frame* 'frame'.
The frame has five columns where "y" is the time series value and "vistors", "wkends",
"incidentRate", and "seasonality" are exogenous inputs.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> schema = [("y", ta.float64),("visitors", ta.float64),("wkends", ta.float64),("incidentRate", ta.float64),("seasonality", ta.float64)]
>>> frame = ta.Frame(ta.UploadRows([[68,278,0,28,0.015132758079119],
...                                 [89,324,0,28,0.0115112433251418],
...                                 [96,318,0,28,0.0190129524583803],
...                                 [98,347,0,28,0.0292307976571017],
...                                 [70,345,1,28,0.0232811662755677],
...                                 [88,335,1,29,0.0306535355961641],
...                                 [76,309,0,29,0.0278080597180392],
...                                 [104,318,0,29,0.0305241957835221],
...                                 [64,308,0,29,0.0247039042146302],
...                                 [89,320,0,29,0.0269026810295449],
...                                 [76,292,0,29,0.0283254189686074],
...                                 [66,295,1,29,0.0230224866502836],
...                                 [84,383,1,21,0.0279373995306813],
...                                 [49,237,0,21,0.0263853217789767],
...                                 [47,210,0,21,0.0230224866502836]],
...                                 schema=schema))
-etc-

</hide>

>>> frame.inspect()
[#]  y      visitors  wkends  incidentRate  seasonality
===========================================================
[0]   68.0     278.0     0.0          28.0  0.0151327580791
[1]   89.0     324.0     0.0          28.0  0.0115112433251
[2]   96.0     318.0     0.0          28.0  0.0190129524584
[3]   98.0     347.0     0.0          28.0  0.0292307976571
[4]   70.0     345.0     1.0          28.0  0.0232811662756
[5]   88.0     335.0     1.0          29.0  0.0306535355962
[6]   76.0     309.0     0.0          29.0   0.027808059718
[7]  104.0     318.0     0.0          29.0  0.0305241957835
[8]   64.0     308.0     0.0          29.0  0.0247039042146
[9]   89.0     320.0     0.0          29.0  0.0269026810295

>>> model = ta.ArxModel()
<progress>

>>> train_output = model.train(frame, "y", ["visitors", "wkends", "incidentRate", "seasonality"], 0, 0, True)
<progress>

>>> train_output
{u'c': 0.0,
 u'coefficients': [0.27583285049358186,
  -13.096710518563603,
  -0.030872283789462572,
  -103.8264674349643]}

>>> predicted_frame = model.predict(frame, "y", ["visitors", "wkends", "incidentRate", "seasonality"])
<progress>

>>> predicted_frame.column_names
[u'y', u'visitors', u'wkends', u'incidentRate', u'seasonality', u'predicted_y']

>>> predicted_frame.inspect(columns=("y","predicted_y"))
[#]  y      predicted_y
=========================
[0]   68.0  74.2459276772
[1]   89.0  87.3102478836
[2]   96.0  84.8763748216
[3]   98.0  91.8146447141
[4]   70.0  78.7839977035
[5]   88.0  75.2293498516
[6]   76.0  81.4498419659
[7]  104.0  83.6503308076
[8]   64.0  81.4963026157
[9]   89.0  84.5780055922

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
u'{"model_details":{"model_type":"ARX Model","model_class":"com.cloudera.sparkts.models.ARXModel","model_reader":"org.trustedanalytics.atk.scoring.models.ARXModelReaderPlugin","custom_values":{}},"input":[{"name":"y","value":"Array[Double]"},{"name":"x_values","value":"Array[Double]"}],"output":[{"name":"y","value":"Array[Double]"},{"name":"x_values","value":"Array[Double]"},{"name":"score","value":"Array[Double]"}]}'
</skip>

The ARX model only supports version 2 of the scoring engine.  In the following example, we are using the ARX model
that was trained and published in the example above.  To keep things simple, we just send the first three rows of
'y' values and the corresponding 'x_values' (visitors, wkends, incidentRate, and seasonality).

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v2/score',json={"records":[{"y":[68.0,89.0,96.0],"x_values":[278.0,324.0,318.0,0.0,0.0,0.0,28.0,28.0,28.0,0.0151327580791,0.0115112433251,0.0190129524584]}]})
</skip>

The 'score' value contains an array of predicted y values.

<skip>
>>> r.text
u'{"data":[{"y":[68.0,89.0,96.0],"x_values":[278.0,324.0,318.0,0.0,0.0,0.0,28.0,28.0,28.0,0.0151327580791,0.0115112433251,0.0190129524584],"score":[74.24592767720993,87.31024788358613,84.8763748215895]}]}'
</skip>
