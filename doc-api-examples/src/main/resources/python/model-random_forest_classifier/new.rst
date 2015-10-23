
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
Consider the following frame containing four columns.

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