Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[-48,1], [-75,1], [-63,1], [-57,1],
...                                 [73,0], [-33,1], [100,0], [-54,1],
...                                 [78,0], [48,0], [-55,1], [23,0], [45,0], [75,0]],
...                                 [("data", ta.float64),("label", str)]))
-etc-

</hide>
Consider the following frame containing three columns.

>>> frame.inspect()
[#]  data   label
=================
[0]  -48.0  1
[1]  -75.0  1
[2]  -63.0  1
[3]  -57.0  1
[4]   73.0  0
[5]  -33.0  1
[6]  100.0  0
[7]  -54.0  1
[8]   78.0  0
[9]   48.0  0

>>> model = ta.SvmModel()
<progress>
>>> train_output = model.train(frame, 'label', ['data'])
<progress>

>>> predicted_frame = model.predict(frame, ['data'])
<progress>
>>> predicted_frame.inspect()
[#]  data   label  predicted_label
==================================
[0]  -48.0  1                    1
[1]  -75.0  1                    1
[2]  -63.0  1                    1
[3]  -57.0  1                    1
[4]   73.0  0                    0
[5]  -33.0  1                    1
[6]  100.0  0                    0
[7]  -54.0  1                    1
[8]   78.0  0                    0
[9]   48.0  0                    0


>>> test_metrics = model.test(predicted_frame, 'predicted_label')
<progress>

>>> test_metrics
Precision: 1.0
Recall: 1.0
Accuracy: 1.0
FMeasure: 1.0
Confusion Matrix:
            Predicted_Pos  Predicted_Neg
Actual_Pos              7              0
Actual_Neg              0              7

>>> model.publish()
<progress>

<skip>
# Take the path to the published model and run it in the Scoring Engine
>>> import requests
>>> headers = {'Content-type': 'application/json', 'Accept': 'application/json,text/plain'}

# posting a request to get the metadata about the model
>>> r =requests.get('http://mymodel.demotrustedanalytics.com/v2/metadata')
>>> r.text
u'{"model_details":{"model_type":"SVM with SGD Model","model_class":"org.trustedanalytics.atk.scoring.models.SVMWithSGDScoreModel","model_reader":"org.trustedanalytics.atk.scoring.models.SVMWithSGDModelReaderPlugin","custom_values":{}},"input":[{"name":"data","value":"Double"}],"output":[{"name":"data","value":"Double"},{"name":"Prediction","value":"Double"}]}'

# Posting a request to version 1 of Scoring Engine supporting strings for requests and response:
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=-48.0', headers=headers)
>>> r.text
u'1.0'

# Posting a request to version 1 with multiple records to score:
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=-48.0&data=73.0', headers=headers)
>>> r.text
u'1.0,0.0'

# Posting a request to version 2 of Scoring Engine supporting Json for requests and responses.
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"data": -48.0}]})
>>> r.text
u'{"data":[{"data":-48.0,"Prediction":1.0}]}'

# posting a request to version 2 with multiple records to score:
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"data": -48.0},{"data": 73.0}]})
>>> r.text
u'{"data":[{"data":-48.0,"Prediction":1.0},{"data":73.0,"Prediction":0.0}]}'
</skip>