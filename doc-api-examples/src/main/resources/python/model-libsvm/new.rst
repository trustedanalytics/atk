
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[1.0, -1.0, -1.0, 1.0],[2.0, -1.0, 0.0, 1.0], [3.0, -1.0, 1.0, 1.0], [4.0, 0.0, -1.0, 1.0],
...                                 [5.0, 0.0, 0.0, 1.0], [6.0, 0.0, 1.0, 1.0], [7.0, 1.0, -1.0, 1.0], [8.0, 1.0, 0.0, 1.0], [9.0, 1.0, 1.0, 1.0]],
...                                 [('idNum', ta.float64), ('tr_row', ta.float64), ('tr_col',ta.float64), ('pos_one',ta.float64)]))
-etc-

</hide>
Consider the following frame containing four columns.

>>> frame.inspect()
    [#]  idNum  tr_row  tr_col  pos_one
    ===================================
    [0]    1.0    -1.0    -1.0      1.0
    [1]    2.0    -1.0     0.0      1.0
    [2]    3.0    -1.0     1.0      1.0
    [3]    4.0     0.0    -1.0      1.0
    [4]    5.0     0.0     0.0      1.0
    [5]    6.0     0.0     1.0      1.0
    [6]    7.0     1.0    -1.0      1.0
    [7]    8.0     1.0     0.0      1.0
    [8]    9.0     1.0     1.0      1.0
>>> model = ta.LibsvmModel()
<progress>
>>> train_output = model.train(frame, "idNum", ["tr_row", "tr_col"],svm_type=2,epsilon=10e-3,gamma=1.0/2,nu=0.1,p=0.1)
<progress>
>>> predicted_frame = model.predict(frame)
<progress>
>>> predicted_frame.inspect()
    [#]  idNum  tr_row  tr_col  pos_one  predicted_label
    ====================================================
    [0]    1.0    -1.0    -1.0      1.0              1.0
    [1]    2.0    -1.0     0.0      1.0              1.0
    [2]    3.0    -1.0     1.0      1.0             -1.0
    [3]    4.0     0.0    -1.0      1.0              1.0
    [4]    5.0     0.0     0.0      1.0              1.0
    [5]    6.0     0.0     1.0      1.0              1.0
    [6]    7.0     1.0    -1.0      1.0              1.0
    [7]    8.0     1.0     0.0      1.0              1.0
    [8]    9.0     1.0     1.0      1.0              1.0
>>> test_obj = model.test(frame, "pos_one",["tr_row", "tr_col"])
<progress>
>>> test_obj.accuracy
0.8888888888888888
>>> test_obj.precision
1.0
>>> test_obj.f_measure
0.9411764705882353
>>> test_obj.recall
0.8888888888888888
>>> score = model.score([3,4])
<progress>
>>> score
-1.0
>>> model.publish()
<progress>

<skip>
# Take the path to the published model and run it in the Scoring Engine
>>> import requests
>>> headers = {'Content-type': 'application/json', 'Accept': 'application/json,text/plain'}

# posting a request to get the metadata about the model
>>> r =requests.get('http://mymodel.demotrustedanalytics.com/v2/metadata')
>>> r.text
u'{"model_details":{"model_type":"LibSvm Model","model_class":"org.trustedanalytics.atk.scoring.models.LibSvmModel","model_reader":"org.trustedanalytics.atk.scoring.models.LibSvmModelReaderPlugin","custom_values":{}},"input":[{"name":"tr_row","value":"Double"},{"name":"tr_col","value":"Double"}],"output":[{"name":"tr_row","value":"Double"},{"name":"tr_col","value":"Double"},{"name":"Prediction","value":"Double"}]}'

# Posting a request to version 1 of Scoring Engine supporting strings for requests and response:
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=2,17,-6', headers=headers)
>>> r.text
u'-1.0'

# Posting a request to version 1 with multiple records to score:
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=2,17,-6&data=0,0,0', headers=headers)
>>> r.text
u'-1.0,1.0'

# Posting a request to version 2 of Scoring Engine supporting Json for requests and responses. In the following example, 'tr_row' and 'tr_col' are the names of the observation columns that the model was trained on:
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"tr_row": 1.0, "tr_col": 2.6}]})
>>> r.text
u'{"data":[{"tr_row":1.0,"tr_col":2.6,"Prediction":-1.0}]}'

# posting a request to version 2 with multiple records to score:
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"tr_row": 1.0, "tr_col": 2.6},{"tr_row": 3.0, "tr_col": 0.6} ]})
>>> r.text
u'{"data":[{"tr_row":1.0,"tr_col":2.6,"Prediction":-1.0},{"tr_row":3.0,"tr_col":0.6,"Prediction":-1.0}]}'
</skip>








