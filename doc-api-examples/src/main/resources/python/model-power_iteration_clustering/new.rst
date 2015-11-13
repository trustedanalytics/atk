Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[1,2,1.0],[1,3,0.3],[2,3,0.3],[3,0,0.03],[0,5,0.01],[5,4,0.3],[5,6,1.0],[4,6,0.3]],
...                                 [('Source', ta.int64), ('Destination', ta.int64), ('Similarity',ta.float64)]))
-etc-

</hide>
Consider the following frame containing three columns denoting the source vertex, destination vertex and corresponding similarity.

>>> frame.inspect()
[#]  Source  Destination  Similarity
====================================
[0]       1            2         1.0
[1]       1            3         0.3
[2]       2            3         0.3
[3]       3            0        0.03
[4]       0            5        0.01
[5]       5            4         0.3
[6]       5            6         1.0
[7]       4            6         0.3

>>> model = ta.PowerIterationClusteringModel()
<progress>
>>> predict_output = model.predict(frame, 'Source', 'Destination', 'Similarity', k=3)
<progress>
>>> predict_output['predicted_frame'].inspect()
[#]  id  cluster
================
[0]   4        3
[1]   0        2
[2]   1        1
[3]   6        1
[4]   3        3
[5]   5        1
[6]   2        1

>>> predict_output['cluster_size']
{u'Cluster:1': 4, u'Cluster:2': 1, u'Cluster:3': 2}
>>> predict_output['number_of_clusters']
3