
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[0,1,0,0], [2,0,0,0], [1,0,1,0], [1,0,2,0], [2,0,0,1], [2,0,0,2]],
...                 [("Class", ta.int32),("Dim_1", ta.int32),("Dim_2", ta.int32),("Dim_3",ta.int32)]))
-etc-

</hide>
Consider the following frame containing four columns.

>>> frame.inspect()
[#]  Class  Dim_1  Dim_2  Dim_3
===============================
[0]      0      1      0      0
[1]      2      0      0      0
[2]      1      0      1      0
[3]      1      0      2      0
[4]      2      0      0      1
[5]      2      0      0      2
>>> model = ta.NaiveBayesModel()
<progress>
>>> model.train(frame, 'Class',['Dim_1','Dim_2','Dim_3'])
<progress>
>>> metrics = model.test(frame, 'Class', ['Dim_1','Dim_2','Dim_3'])
<progress>
>>> metrics['f_measure']
1.0
>>> metrics['recall']
1.0
>>> metrics['accuracy']
1.0
>>> metrics['precision']
1.0
>>> metrics['confusion_matrix']
{u'column_labels': [u'pos', u'neg'],
 u'matrix': [[2, 0], [0, 4]],
 u'row_labels': [u'pos', u'neg']}
