Examples
--------
For this example, the Frame object *my_frame* accesses a frame with 4 columns
columns *column_a*, *column_b*, *column_c* and *column_d* and drops 2 columns *column_b* and *column_d* using drop columns.


    <hide>

    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-
    
    >>> sc=[("column_a", str), ("column_b", ta.int32), ("column_c", str), ("column_d", ta.int32)]
    >>> rows = [["Alameda", 1, "CA", 7], ["Princeton", 2, "NJ", 6], ["NewYork", 3 , "NY", 9]]
    >>> my_frame = ta.Frame(ta.UploadRows (rows, sc))
    -etc-

    </hide>

    >>> print my_frame.schema
    [(u'column_a', <type 'unicode'>), (u'column_b', <type 'numpy.int32'>), (u'column_c', <type 'unicode'>), (u'column_d', <type 'numpy.int32'>)]


Eliminate columns *column_b* and *column_d*:

    >>> my_frame.drop_columns(["column_b", "column_d"])
    >>> print my_frame.schema
    [(u'column_a', <type 'unicode'>), (u'column_c', <type 'unicode'>)]

Now the frame only has the columns *column_a* and *column_c*.
For further examples, see: ref:`example_frame.drop_columns`.


