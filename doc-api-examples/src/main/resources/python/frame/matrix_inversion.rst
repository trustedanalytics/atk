Examples
--------
Consider Frame *my_frame*, which contains the data

    <hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-

    >>> s = [("x1", ta.float64), ("x2", ta.float64), ("x3", ta.float64)]
    >>> rows = [ [1.0, 0.0, 0.0], [2.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
    >>> my_frame = ta.Frame(ta.UploadRows (rows, s))
    -etc-

    </hide>
    >>> my_frame.inspect()
    [#]  x1   x2   x3
    ==================
    [0]  1.0  0.0  0.0
    [1]  2.0  1.0  0.0
    [2]  0.0  0.0  1.0

    >>> inv_matrix = my_frame.invert_matrix(my_frame.column_names)
    <progress>

    >>> inv_matrix.inspect()
    [#]  x1    x2                 x3
    =================================
    [0]   1.0  3.88578058619e-16  0.0
    [1]  -2.0                1.0  0.0
    [2]   0.0                0.0  1.0





