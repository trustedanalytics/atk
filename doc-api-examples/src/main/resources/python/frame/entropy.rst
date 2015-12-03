Consider the following sample data set in *frame* 'frame' containing several numbers.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

>>> frame = ta.Frame(ta.UploadRows([[0,1], [1,2], [2,4], [4,8]], [('data', ta.int32), ('weight', ta.int32)]))
-etc-

</hide>
Given a frame of coin flips, half heads and half tails, the entropy is simply ln(2):

>>> frame.inspect()
[#]  data  weight
=================
[0]     0       1
[1]     1       2
[2]     2       4
[3]     4       8
>>> entropy = frame.entropy("data", "weight")
<progress>

>>> "%0.8f" % entropy
'1.13691659'



If we have more choices and weights, the computation is not as simple.
An on-line search for "Shannon Entropy" will provide more detail.

<hide>
>>> frame = ta.Frame(ta.UploadRows([["H"], ["T"], ["H"], ["T"], ["H"], ["T"], ["H"], ["T"], ["H"], ["T"]], [('data', str)]))
-etc-

</hide>
Given a frame of coin flips, half heads and half tails, the entropy is simply ln(2):

>>> frame.inspect()
[#]  data
=========
[0]  H
[1]  T
[2]  H
[3]  T
[4]  H
[5]  T
[6]  H
[7]  T
[8]  H
[9]  T
>>> entropy = frame.entropy("data")
<progress>
>>> "%0.8f" % entropy
'0.69314718'

