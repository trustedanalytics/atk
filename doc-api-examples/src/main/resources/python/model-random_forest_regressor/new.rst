
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[1,19.8446136104,2.2985856384],[1,16.8973559126,2.6933495054],
...                                 [1,5.5548729596,2.7777687995],[0,46.1810010826,3.1611961917],
...                                 [0,44.3117586448,3.3458963222],[0,34.6334526911,3.6429838715]],
...                                 [('Class', int), ('Dim_1', ta.float64), ('Dim_2',ta.float64)]))
-etc-

</hide>
Consider the following frame containing three columns.

>>> frame.inspect()
[#]  Class  Dim_1          Dim_2
=======================================
[0]      1  19.8446136104  2.2985856384
[1]      1  16.8973559126  2.6933495054
[2]      1   5.5548729596  2.7777687995
[3]      0  46.1810010826  3.1611961917
[4]      0  44.3117586448  3.3458963222
[5]      0  34.6334526911  3.6429838715
>>> model = ta.RandomForestRegressorModel()
<progress>
>>> train_output = model.train(frame, 'Class', ['Dim_1', 'Dim_2'], num_trees=1, impurity="variance", max_depth=4, max_bins=100)
<progress>
<skip>
>>> train_output
{u'impurity': u'variance', u'max_bins': 100, u'observation_columns': [u'Dim_1', u'Dim_2'], u'num_nodes': 3, u'max_depth': 4, u'seed': -1632404927, u'num_trees': 1, u'label_column': u'Class', u'feature_subset_category': u'all'}
</skip>
>>> train_output['num_nodes']
3
>>> train_output['label_column']
u'Class'
>>> predicted_frame = model.predict(frame, ['Dim_1', 'Dim_2'])
<progress>
>>> predicted_frame.inspect()
[#]  Class  Dim_1          Dim_2         predicted_value
========================================================
[0]      1  19.8446136104  2.2985856384                1.0
[1]      1  16.8973559126  2.6933495054                1.0
[2]      1   5.5548729596  2.7777687995                1.0
[3]      0  46.1810010826  3.1611961917                0.0
[4]      0  44.3117586448  3.3458963222                0.0
[5]      0  34.6334526911  3.6429838715                0.0
>>> model.publish()
<progress>

<skip>
# Take the path to the published model and run it in the Scoring Engine
>>> import requests
>>> headers = {'Content-type': 'application/json', 'Accept': 'application/json,text/plain'}

# posting a request to get the metadata about the model
>>> r =requests.get('http://mymodel.demotrustedanalytics.com/v2/metadata')
>>> r.text
u'{"model_details":{"model_type":"Random Forest Regressor Model","model_class":"org.trustedanalytics.atk.scoring.models.RandomForestRegressorScoreModel","model_reader":"org.trustedanalytics.atk.scoring.models.RandomForestRegressorModelReaderPlugin","custom_values":{}},"input":[{"name":"Dim_1","value":"Double"},{"name":"Dim_2","value":"Double"}],"output":[{"name":"Dim_1","value":"Double"},{"name":"Dim_2","value":"Double"},{"name":"Prediction","value":"Double"}]}'

# Posting a request to version 1 of Scoring Engine supporting strings for requests and response:
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=19.8446136, 2.2985856384', headers=headers)
>>> r.text
u'1.0'

# Posting a request to version 1 with multiple records to score:
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=19.8446136, 2.2985856384&data=46.1810010826, 3.1611961917', headers=headers)
>>> r.text
u'1.0,0.0'

# Posting a request to version 2 of Scoring Engine supporting Json for requests and responses.
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"Dim_1": 19.8446136, "Dim_2": 2.2985856384}]})
>>> r.text
u'{"data":[{"Dim_1":19.8446136,"Dim_2":2.2985856384,"Prediction":1.0}]}'

# posting a request to version 2 with multiple records to score:
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"Dim_1": 19.8446136, "Dim_2": 2.2985856384}, {"Dim_1": 46.1810010826, "Dim_2": 3.1611961917}]})
>>> r.text
u'{"data":[{"Dim_1":19.8446136,"Dim_2":2.2985856384,"Prediction":1.0},{"Dim_1":46.1810010826,"Dim_2":3.1611961917,"Prediction":0.0}]}'
</skip>