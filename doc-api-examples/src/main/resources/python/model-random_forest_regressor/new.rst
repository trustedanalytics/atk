
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