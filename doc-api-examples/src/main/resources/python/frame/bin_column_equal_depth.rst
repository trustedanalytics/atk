Examples
--------
Given a frame with column *a* accessed by a Frame object *my_frame*:

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

>>> my_frame = ta.Frame(ta.UploadRows([[1],[1],[2],[3],[5],[8],[13],[21],[34],[55],[89]],
... [('a', ta.int32)]))
-etc-

</hide>
>>> my_frame.inspect( n=11 )
[##]  a 
========
[0]    1
[1]    1
[2]    2
[3]    3
[4]    5
[5]    8
[6]   13
[7]   21
[8]   34
[9]   55
[10]  89


Modify the frame, adding a column showing what bin the data is in.
The data should be grouped into a maximum of five bins.
Note that each bin will have the same quantity of members (as much as
possible):

>>> cutoffs = my_frame.bin_column_equal_depth('a', 5, 'aEDBinned')
<progress>
>>> my_frame.inspect( n=11 )
[##]  a   aEDBinned
===================
[0]    1          0
[1]    1          0
[2]    2          1
[3]    3          1
[4]    5          2
[5]    8          2
[6]   13          3
[7]   21          3
[8]   34          4
[9]   55          4
[10]  89          4

>>> print cutoffs
[1.0, 2.0, 5.0, 13.0, 34.0, 89.0]
