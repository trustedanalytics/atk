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

>>> model = ta.NaiveBayesModel()
<progress>
>>> model.train(frame, 'Class', ['Dim_1', 'Dim_2'], lambda_parameter=0.9)
<progress>
>>> predicted_frame = model.predict(frame, ['Dim_1', 'Dim_2'])
<progress>
>>> predicted_frame.inspect()
[#]  Class  Dim_1          Dim_2         predicted_class
========================================================
[0]      1  19.8446136104  2.2985856384              0.0
[1]      1  16.8973559126  2.6933495054              1.0
[2]      1   5.5548729596  2.7777687995              1.0
[3]      0  46.1810010826  3.1611961917              0.0
[4]      0  44.3117586448  3.3458963222              0.0
[5]      0  34.6334526911  3.6429838715              0.0

>>> test_metrics = model.test(frame, 'Class', ['Dim_1','Dim_2'])
<progress>
>>> test_metrics
Precision: 1.0
Recall: 0.666666666667
Accuracy: 0.833333333333
FMeasure: 0.8
Confusion Matrix:
            Predicted_Pos  Predicted_Neg
Actual_Pos              2              1
Actual_Neg              0              3
