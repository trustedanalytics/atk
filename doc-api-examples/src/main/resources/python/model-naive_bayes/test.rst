
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> frame = ta.Frame(ta.UploadRows([[0,1,0,0], [2,0,0,0], [1,0,1,0], [1,0,2,0], [2,0,0,1], [2,0,0,2]],
            [("Class", ta.int32),("Dim_1", ta.int32),("Dim_2", ta.int32),("Dim_3",ta.int32)]))
-etc-

</hide>

>>> model = ta.NaiveBayesModel()
>>> model.train(frame, 'Class',['Dim_1','Dim_2','Dim_3'])
>>> metrics = model.test(frame, 'Class', ['Dim_1','Dim_2','Dim_3'])
<progress>
>>> metrics['f_measure']
0.66666666666666663

>>> metrics['recall']
0.5

>>> metrics['accuracy']
0.75

>>> metrics['precision']
1.0

>>> metrics['confusion_matrix']
{u'column_labels': [u'pos', u'neg'],
 u'matrix': [[2, 0], [0, 3]],
 u'row_labels': [u'pos', u'neg']}
