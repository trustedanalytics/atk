
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[1,19.8446136104,2.2985856384],[1,16.8973559126,2.6933495054],
...                                 [1,5.5548729596,2.7777687995],[0,46.1810010826,3.1611961917],
...                                 [0,44.3117586448,3.3458963222],[0,34.6334526911,3.6429838715]],
...                                 [('Value', int), ('Dim_1', ta.float64), ('Dim_2',ta.float64)]))
-etc-

</hide>
Consider the following frame containing three columns.

>>> frame.inspect()
[#]  Value  Dim_1          Dim_2
=======================================
[0]      1  19.8446136104  2.2985856384
[1]      1  16.8973559126  2.6933495054
[2]      1   5.5548729596  2.7777687995
[3]      0  46.1810010826  3.1611961917
[4]      0  44.3117586448  3.3458963222
[5]      0  34.6334526911  3.6429838715
>>> model = ta.H2oRandomForestRegressorModel()
<progress>
>>> train_output = model.train(frame, 'Value', ['Dim_1', 'Dim_2'], num_trees=1, max_depth=4, num_bins=2)
<progress>
<skip>
>>> train_output
mse: 0.0
rmse: 0.0
r2: 1.0
varimp:
{u'Dim_2': 1.5, u'Dim_1': 0.0}
</skip>
>>> train_output.mse
0.0
<skip>
>>> train_output.varimp_as_pandas()
 	variable 	importance
0 	Dim_2 	1.5
1 	Dim_1 	0.0
</skip>
>>> predicted_frame = model.predict(frame, ['Dim_1', 'Dim_2'])
<progress>
>>> predicted_frame.inspect()
[#]  Value  Dim_1          Dim_2         predicted_value
========================================================
[0]      1  19.8446136104  2.2985856384              1.0
[1]      1  16.8973559126  2.6933495054              1.0
[2]      1   5.5548729596  2.7777687995              1.0
[3]      0  46.1810010826  3.1611961917              0.0
[4]      0  44.3117586448  3.3458963222              0.0
[5]      0  34.6334526911  3.6429838715              0.0
>>> test_output = model.test(frame, 'Value')
<skip>
>>> test_output
mse: 0.0
rmse: 0.0
r2: 1.0
</skip>
>>> test_output.r2
1.0
>>> model.publish()
<progress>

Take the path to the published model and run it in the Scoring Engine

<skip>
>>> import requests
>>> headers = {'Content-type': 'application/json', 'Accept': 'application/json,text/plain'}
</skip>

Posting a request to get the metadata about the model

<skip>
>>> r =requests.get('http://mymodel.demotrustedanalytics.com/v2/metadata')
>>> r.text
u'{"model_details":{"model_type":"H2O Random Forest Regressor Model","model_class":"org.trustedanalytics.atk.scoring.models.H2oRandomForestRegressorScoreModel","model_reader":"org.trustedanalytics.atk.scoring.models.H2oRandomForestRegressorModelReaderPlugin","custom_values":{}},"input":[{"name":"Dim_1","value":"Double"},{"name":"Dim_2","value":"Double"}],"output":[{"name":"Dim_1","value":"Double"},{"name":"Dim_2","value":"Double"},{"name":"Prediction","value":"Double"}]}'
</skip>

Posting a request to version 1 of Scoring Engine supporting strings for requests and response:

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=19.8446136,2.2985856384', headers=headers)
>>> r.text
u'1.0'
</skip>

Posting a request to version 1 with multiple records to score:

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=19.8446136,2.2985856384&data=46.1810010826,3.1611961917', headers=headers)
>>> r.text
u'1.0,0.0'
</skip>

Posting a request to version 2 of Scoring Engine supporting Json for requests and responses.

<skip>
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"Dim_1": 19.8446136, "Dim_2": 2.2985856384}]})
>>> r.text
u'{"data":[{"Dim_1":19.8446136,"Dim_2":2.2985856384,"Prediction":1.0}]}'
</skip>

Posting a request to version 2 with multiple records to score:

<skip>
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"Dim_1": 19.8446136, "Dim_2": 2.2985856384}, {"Dim_1": 46.1810010826, "Dim_2": 3.1611961917}]})
>>> r.text
u'{"data":[{"Dim_1":19.8446136,"Dim_2":2.2985856384,"Prediction":1.0},{"Dim_1":46.1810010826,"Dim_2":3.1611961917,"Prediction":0.0}]}'
</skip>