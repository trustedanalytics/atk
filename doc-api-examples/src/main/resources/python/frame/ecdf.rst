
Consider the following sample data set in *frame* 'frame' containing several numbers.

<hide>
>>> frame = ta.Frame(ta.UploadRows([[1], [3], [1], [0], [2], [1], [4], [3]], [('numbers', ta.int32)]))
-etc-

</hide>

>>> frame.inspect()
[#]  numbers
============
[0]        1
[1]        3
[2]        1
[3]        0
[4]        2
[5]        1
[6]        4
[7]        3
>>> ecdf_frame = frame.ecdf('numbers')
<progress>
>>> ecdf_frame.inspect()
[#]  numbers  numbers_ECDF
==========================
[0]        0         0.125
[1]        1           0.5
[2]        2         0.625
[3]        3         0.875
[4]        4           1.0

