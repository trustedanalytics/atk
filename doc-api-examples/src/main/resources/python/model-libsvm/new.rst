
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
>>> predictedscore = model.score([3,4])
<progress>
>>> score
-1.0
>>> model.publish()
<progress>








