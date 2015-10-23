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
The data should be separated into a maximum of five bins and the bin cutoffs
should be evenly spaced.
Note that there may be bins with no members:

>>> cutoffs = my_frame.bin_column_equal_width('a', 5, 'aEWBinned')
<progress>
>>> my_frame.inspect( n=11 )
[##]  a   aEWBinned
===================
[0]    1          0
[1]    1          0
[2]    2          0
[3]    3          0
[4]    5          0
[5]    8          0
[6]   13          0
[7]   21          1
[8]   34          1
[9]   55          3
[10]  89          4

The method returns a list of 6 cutoff values that define the edges of each bin.
Note that difference between the cutoff values is constant:

>>> print cutoffs
[1.0, 18.6, 36.2, 53.8, 71.4, 89.0]

