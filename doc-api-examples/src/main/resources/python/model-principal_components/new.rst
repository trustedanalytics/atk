
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[2.6,1.7,0.3,1.5,0.8,0.7],
...                                 [3.3,1.8,0.4,0.7,0.9,0.8],
...                                 [3.5,1.7,0.3,1.7,0.6,0.4],
...                                 [3.7,1.0,0.5,1.2,0.6,0.3],
...                                 [1.5,1.2,0.5,1.4,0.6,0.4]],
...                                 [("1", ta.float64),("2", ta.float64),("3", ta.float64),
...                                  ("4", ta.float64),("5", ta.float64),("6", ta.float64)]))
-etc-

</hide>
Consider the following frame containing six columns.

>>> frame.inspect()
[#]  1    2    3    4    5    6
=================================
[0]  2.6  1.7  0.3  1.5  0.8  0.7
[1]  3.3  1.8  0.4  0.7  0.9  0.8
[2]  3.5  1.7  0.3  1.7  0.6  0.4
[3]  3.7  1.0  0.5  1.2  0.6  0.3
[4]  1.5  1.2  0.5  1.4  0.6  0.4
>>> model = ta.PrincipalComponentsModel()
<progress>
>>> train_output = model.train(frame, ['1','2','3','4','5','6'], mean_centered=True, k=6)
<progress>
<skip>
>>> train_output
{u'k': 6, u'column_means': [2.92, 1.48, 0.4, 1.3, 0.7, 0.52], u'observation_columns': [u'1', u'2', u'3', u'4', u'5', u'6'], u'mean_centered': True, u'right_singular_vectors': [[-0.9906468642089332, 0.11801374544146297, 0.025647010353320242, 0.048525096275535286, -0.03252674285233843, 0.02492194235385788], [-0.07735139793384983, -0.6023104604841424, 0.6064054412059493, -0.4961696216881456, -0.12443126544906798, -0.042940400527513106], [0.028850639537397756, 0.07268697636708575, -0.2446393640059097, -0.17103491337994586, -0.9368360903028429, 0.16468461966702994], [0.10576208410025369, 0.5480329468552815, 0.75230590898727, 0.2866144016081251, -0.20032699877119212, 0.015210798298156058], [-0.024072151446194606, -0.30472267167437633, -0.01125936644585159, 0.48934541040601953, -0.24758962014033054, -0.7782533654748628], [-0.0061729539518418355, -0.47414707747028795, 0.07533458226215438, 0.6329307498105832, -0.06607181431092408, 0.6037419362435869]], u'singular_values': [1.8048170096632419, 0.8835344148403882, 0.7367461843294286, 0.15234027471064404, 5.90167578565564e-09, 4.478916578455115e-09]}
</skip>
<skip>
>>> train_output['column_means']
[2.92, 1.48, 0.4, 1.3, 0.7, 0.52]
</skip>
>>> predicted_frame = model.predict(frame, mean_centered=True, t_squared_index=True, observation_columns=['1','2','3','4','5','6'], c=3)
<progress>
>>> predicted_frame.inspect()
[#]  1    2    3    4    5    6    p_1              p_2
===================================================================
[0]  2.6  1.7  0.3  1.5  0.8  0.7   0.314738695012  -0.183753549226
[1]  3.3  1.8  0.4  0.7  0.9  0.8  -0.471198363594  -0.670419608227
[2]  3.5  1.7  0.3  1.7  0.6  0.4  -0.549024749481   0.235254068619
[3]  3.7  1.0  0.5  1.2  0.6  0.3  -0.739501762517   0.468409769639
[4]  1.5  1.2  0.5  1.4  0.6  0.4    1.44498618058   0.150509319195
<BLANKLINE>
[#]  p_3              t_squared_index
=====================================
[0]   0.312561560113   0.253649649849
[1]  -0.228746130528   0.740327252782
[2]   0.465756549839   0.563086507007
[3]  -0.386212142456   0.723748467549
[4]  -0.163359836968   0.719188122813
>>> model.publish()
<progress>

<skip>
# Take the path to the published model and run it in the Scoring Engine
>>> import requests
>>> headers = {'Content-type': 'application/json', 'Accept': 'application/json,text/plain'}

# posting a request to get the metadata about the model
>>> r =requests.get('http://mymodel.demotrustedanalytics.com/v2/metadata')
>>> r.text
u'{"model_details":{"model_type":"Principal Components Model","model_class":"org.trustedanalytics.atk.scoring.models.PrincipalComponentsScoreModel","model_reader":"org.trustedanalytics.atk.scoring.models.PrincipalComponentsModelReaderPlugin","custom_values":{}},"input":[{"name":"1","value":"Double"},{"name":"2","value":"Double"},{"name":"3","value":"Double"},{"name":"4","value":"Double"},{"name":"5","value":"Double"},{"name":"6","value":"Double"}],"output":[{"name":"1","value":"Double"},{"name":"2","value":"Double"},{"name":"3","value":"Double"},{"name":"4","value":"Double"},{"name":"5","value":"Double"},{"name":"6","value":"Double"},{"name":"principal_components","value":"List[Double]"},{"name":"t_squared_index","value":"Double"}]}'

# Posting a request to version 1 of Scoring Engine supporting strings for requests and response:
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=2.6,  1.7,  0.3,  1.5,  0.8,  0.7', headers=headers)
>>> r.text
u'0.8000000000000014'

# Posting a request to version 1 with multiple records to score:
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v1/score?data=2.6,  1.7,  0.3,  1.5,  0.8,  0.7&data=1.5,  1.2,  0.5,  1.4,  0.6,  0.4', headers=headers)
>>> r.text
u'0.8000000000000014,0.7999999999999993'

# Posting a request to version 2 of Scoring Engine supporting Json for requests and responses.
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"1": 2.6, "2": 1.7, "3": 0.3, "4": 1.5, "5": 0.8, "6": 0.7}]})
>>> r.text
u'{"data":[{"t_squared_index":0.8000000000000014,"4":1.5,"5":0.8,"6":0.7,"1":2.6,"principal_components":[0.31473869501177154,-0.18375354922552106,0.31256156011289404,0.11260310008656331,-1.8388068845354155E-16,2.0816681711721685E-16],"2":1.7,"3":0.3}]}'

# posting a request to version 2 with multiple records to score:
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"1": 2.6, "2": 1.7, "3": 0.3, "4": 1.5, "5": 0.8, "6": 0.7}, {"1": 1.5, "2": 1.2, "3": 0.5, "4": 1.4, "5": 0.6, "6":0.4}]})
>>> r.text
u'{"data":[{"t_squared_index":0.8000000000000014,"4":1.5,"5":0.8,"6":0.7,"1":2.6,"principal_components":[0.31473869501177154,-0.18375354922552106,0.31256156011289404,0.11260310008656331,-1.8388068845354155E-16,2.0816681711721685E-16],"2":1.7,"3":0.3},{"t_squared_index":0.7999999999999993,"4":1.4,"5":0.6,"6":0.4,"1":1.5,"principal_components":[1.4449861805807689,0.15050931919479138,-0.16335983696811784,-0.04330642483363326,7.632783294297951E-17,6.938893903907228E-17],"2":1.2,"3":0.5}]}'
</skip>