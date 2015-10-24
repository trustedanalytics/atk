Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[0,0],[1, 2.5],[2, 5.0],[3, 7.5],[4, 10],[5, 12.5],[6, 15.0],[7, 17.5], [8, 20],[9, 22.5]],[("x1", ta.float64),("y", ta.float64)]))
-etc-

</hide>
Consider the following frame containing two columns.

>>> frame.inspect()
[#]  x1   y
==============
[0]  0.0   0.0
[1]  1.0   2.5
[2]  2.0   5.0
[3]  3.0   7.5
[4]  4.0  10.0
[5]  5.0  12.5
[6]  6.0  15.0
[7]  7.0  17.5
[8]  8.0  20.0
[9]  9.0  22.5

>>> model = ta.LinearRegressionModel()
<progress>
>>> model.train(frame, "y", ["x1"], intercept=True, num_iterations=100)
<progress>
>>> predicted_frame = model.predict(frame, ["x1"])
<progress>
<skip>
>>> predicted_frame.inspect()
[#]  x1   y     predicted_value
==================================
[0]  0.0   0.0  -3.74940273364e+55
[1]  1.0   2.5  -2.72603544372e+56
[2]  2.0   5.0  -5.07713061407e+56
[3]  3.0   7.5  -7.42822578442e+56
[4]  4.0  10.0  -9.77932095478e+56
[5]  5.0  12.5  -1.21304161251e+57
[6]  6.0  15.0  -1.44815112955e+57
[7]  7.0  17.5  -1.68326064658e+57
[8]  8.0  20.0  -1.91837016362e+57
[9]  9.0  22.5  -2.15347968065e+57
</skip>