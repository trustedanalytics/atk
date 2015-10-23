
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
Consider the following frame containing four columns.

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
>>> train_output['column_means']
[2.92, 1.48, 0.4, 1.2999999999999998, 0.7000000000000001, 0.5199999999999999]
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