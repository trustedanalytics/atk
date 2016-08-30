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


>>> model = ta.LassoModel()
<progress>

>>> model.train(frame, 'y', ['x1'])
<progress>

>>> predicted_frame = model.predict(frame)
<progress>
>>> predicted_frame.inspect()
[#]  x1   y      predicted_value
================================
[0]  0.0    0.0              0.0
[1]  1.0    2.5     2.4387285895
[2]  2.0    5.0    4.87745717901
[3]  3.0    7.5    7.31618576851
[4]  4.0   10.0    9.75491435802
[5]  5.0   12.5    12.1936429475
[6]  6.0   13.0     14.632371537
[7]  7.0  17.15    17.0711001265
[8]  8.0   18.5     19.509828716
[9]  9.0   23.5    21.9485573055

>>> test_metrics = model.test(predicted_frame, 'predicted_value')
<progress>

>>> test_metrics
{u'mean_squared_error': 0.0, u'r_2': 1.0, u'mean_absolute_error': 0.0, u'explained_variance': 49.066026349445146, u'root_mean_squared_error': 0.0}

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
u'{"model_details":{"model_type":"SVM with SGD Model","model_class":"org.trustedanalytics.atk.scoring.models.SVMWithSGDScoreModel","model_reader":"org.trustedanalytics.atk.scoring.models.SVMWithSGDModelReaderPlugin","custom_values":{}},"input":[{"name":"data","value":"Double"}],"output":[{"name":"data","value":"Double"},{"name":"Prediction","value":"Double"}]}'
</skip>

Posting a request to version 1 of Scoring Engine supporting strings for requests and response:

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=-48.0', headers=headers)
>>> r.text
u'1.0'
</skip>

Posting a request to version 1 with multiple records to score:

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=-48.0&data=73.0', headers=headers)
>>> r.text
u'1.0,0.0'
</skip>

Posting a request to version 2 of Scoring Engine supporting Json for requests and responses.

<skip>
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"data": -48.0}]})
>>> r.text
u'{"data":[{"data":-48.0,"Prediction":1.0}]}'
</skip>

Posting a request to version 2 with multiple records to score:

<skip>
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"data": -48.0},{"data": 73.0}]})
>>> r.text
u'{"data":[{"data":-48.0,"Prediction":1.0},{"data":73.0,"Prediction":0.0}]}'
</skip>