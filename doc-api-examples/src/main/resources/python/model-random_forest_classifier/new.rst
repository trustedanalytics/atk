
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
>>> model = ta.RandomForestClassifierModel()
<progress>
>>> train_output = model.train(frame, 'Class', ['Dim_1', 'Dim_2'], num_classes=2, num_trees=1, impurity="entropy", max_depth=4, max_bins=100)
<progress>
<skip>
>>> train_output
{u'impurity': u'entropy', u'max_bins': 100, u'observation_columns': [u'Dim_1', u'Dim_2'], u'num_nodes': 3, u'max_depth': 4, u'seed': 157264076, u'num_trees': 1, u'label_column': u'Class', u'feature_subset_category': u'all', u'num_classes': 2}
</skip>
>>> train_output['num_nodes']
3
>>> train_output['label_column']
u'Class'
>>> predicted_frame = model.predict(frame, ['Dim_1', 'Dim_2'])
<progress>
>>> predicted_frame.inspect()
[#]  Class  Dim_1          Dim_2         predicted_class
========================================================
[0]      1  19.8446136104  2.2985856384                1
[1]      1  16.8973559126  2.6933495054                1
[2]      1   5.5548729596  2.7777687995                1
[3]      0  46.1810010826  3.1611961917                0
[4]      0  44.3117586448  3.3458963222                0
[5]      0  34.6334526911  3.6429838715                0
>>> test_metrics = model.test(frame, 'Class', ['Dim_1','Dim_2'])
<progress>
>>> test_metrics
Precision: 1.0
Recall: 1.0
Accuracy: 1.0
FMeasure: 1.0
Confusion Matrix:
            Predicted_Pos  Predicted_Neg
Actual_Pos              3              0
Actual_Neg              0              3
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
u'{"model_details":{"model_type":"Random Forest Classifier Model","model_class":"org.trustedanalytics.atk.scoring.models.RandomForestClassifierScoreModel","model_reader":"org.trustedanalytics.atk.scoring.models.RandomForestClassifierModelReaderPlugin","custom_values":{}},"input":[{"name":"Dim_1","value":"Double"},{"name":"Dim_2","value":"Double"}],"output":[{"name":"Dim_1","value":"Double"},{"name":"Dim_2","value":"Double"},{"name":"Prediction","value":"Double"}]}'
</skip>

Posting a request to version 1 of Scoring Engine supporting strings for requests and response:

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=19.8446, 2.2985856', headers=headers)
>>> r.text
u'1.0'
</skip>

Posting a request to version 1 with multiple records to score:

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=19.8446, 2.2985856&data=46.1810010826, 3.1611961917', headers=headers)
>>> r.text
u'1.0,0.0'
</skip>

Posting a request to version 2 of Scoring Engine supporting Json for requests and responses.

<skip>
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"Dim_1": 19.8446, "Dim_2": 2.2985856}]})
>>> r.text
u'{"data":[{"Dim_1":19.8446,"Dim_2":2.2985856,"Prediction":1.0}]}'
</skip>

Posting a request to version 2 with multiple records to score:

<skip>
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"Dim_1": 19.8446, "Dim_2": 2.2985856}, {"Dim_1": 46.1810010826, "Dim_2": 3.1611961917}]})
>>> r.text
u'{"data":[{"Dim_1":19.8446,"Dim_2":2.2985856,"Prediction":1.0},{"Dim_1":46.1810010826,"Dim_2":3.1611961917,"Prediction":0.0}]}'
</skip>