Examples
--------
For these examples, we will use a frame with column *a* accessed by a Frame
object *my_frame*:

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

Modify the frame with a column showing what bin the data is in.
The data values should use strict_binning:

>>> my_frame.bin_column('a', [5,12,25,60], include_lowest=True,
... strict_binning=True, bin_column_name='binned')
<progress>
>>> my_frame.inspect( n=11 )
[##]  a   binned
================
[0]    1      -1
[1]    1      -1
[2]    2      -1
[3]    3      -1
[4]    5       0
[5]    8       0
[6]   13       1
[7]   21       1
[8]   34       2
[9]   55       2
[10]  89      -1

<hide>
>>> my_frame.drop_columns('binned')
-etc-

</hide>
Modify the frame with a column showing what bin the data is in.
The data value should not use strict_binning:


>>> my_frame.bin_column('a', [5,12,25,60], include_lowest=True,
... strict_binning=False, bin_column_name='binned')
<progress>
>>> my_frame.inspect( n=11 )
[##]  a   binned
================
[0]    1       0
[1]    1       0
[2]    2       0
[3]    3       0
[4]    5       0
[5]    8       0
[6]   13       1
[7]   21       1
[8]   34       2
[9]   55       2
[10]  89       2

<hide>
>>> my_frame.drop_columns('binned')
-etc-

</hide>
Modify the frame with a column showing what bin the data is in.
The bins should be lower inclusive:

>>> my_frame.bin_column('a', [1,5,34,55,89], include_lowest=True,
... strict_binning=False, bin_column_name='binned')
<progress>
>>> my_frame.inspect( n=11 )
[##]  a   binned
================
[0]    1       0
[1]    1       0
[2]    2       0
[3]    3       0
[4]    5       1
[5]    8       1
[6]   13       1
[7]   21       1
[8]   34       2
[9]   55       3
[10]  89       3

<hide>
>>> my_frame.drop_columns('binned')
-etc-

</hide>
Modify the frame with a column showing what bin the data is in.
The bins should be upper inclusive:

>>> my_frame.bin_column('a', [1,5,34,55,89], include_lowest=False,
... strict_binning=True, bin_column_name='binned')
<progress>
>>> my_frame.inspect( n=11 )
[##]  a   binned
================
[0]    1       0
[1]    1       0
[2]    2       0
[3]    3       0
[4]    5       0
[5]    8       1
[6]   13       1
[7]   21       1
[8]   34       1
[9]   55       2
[10]  89       3

