Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[3, 2, 1],[6, 4, 2],[9, 6, 3],[12, 8, 4],[15, 10, 5],[18, 12, 6],[21, 14, 7], [24, 16, 8],[27, 18, 9]],[("y", ta.int64),("x1", ta.int64),("x2", ta.int64)]))
-etc-

</hide>
Consider the following frame containing two columns.

>>> frame.inspect()
[#]  y   x1  x2
===============
[0]   3   2   1
[1]   6   4   2
[2]   9   6   3
[3]  12   8   4
[4]  15  10   5
[5]  18  12   6
[6]  21  14   7
[7]  24  16   8
[8]  27  18   9

>>> model = ta.LinearRegressionModel()
<progress>
>>> model.train(frame, "y", ["x1", "x2"], intercept=False, num_iterations=20)
<progress>
>>> predicted_frame = model.predict(frame, ["x1", "x2"])
<progress>
>>> predicted_frame.column_names
[u'y', u'x1', u'x2', u'predicted_value']
