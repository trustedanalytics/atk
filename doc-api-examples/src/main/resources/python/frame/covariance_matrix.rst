Examples
--------
Consider Frame *my_frame*, which contains the data

    <hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-

    >>> s = [("idnum", ta.int32), ("x1", ta.float32), ("x2", ta.float32), ("x3", ta.float32), ("x4", ta.float32)]
    >>> rows = [ [0, 1.0, 4.0, 0.0, -1.0], [1, 2.0, 3.0, 0.0, -1.0], [2, 3.0, 2.0, 1.0, -1.0], [3, 4.0, 1.0, 2.0, -1.0], [4, 5.0, 0.0, 2.0, -1.0]]
    >>> my_frame = ta.Frame(ta.UploadRows (rows, s))
    -etc-

    </hide>
    >>> my_frame.inspect()
     [#]  idnum  x1   x2   x3   x4
    ===============================
    [0]      0  1.0  4.0  0.0  -1.0
    [1]      1  2.0  3.0  0.0  -1.0
    [2]      2  3.0  2.0  1.0  -1.0
    [3]      3  4.0  1.0  2.0  -1.0
    [4]      4  5.0  0.0  2.0  -1.0


my_frame.covariance_matrix computes the covariance on each pair of columns in the user-provided list.

    >>> cov_matrix = my_frame.covariance_matrix(my_frame.column_names)
    <progress>

    The resulting table (specifying all columns) is:

    >>> cov_matrix.inspect()
    [#]  idnum  x1    x2    x3    x4
    =================================
    [0]    2.5   2.5  -2.5   1.5  0.0
    [1]    2.5   2.5  -2.5   1.5  0.0
    [2]   -2.5  -2.5   2.5  -1.5  0.0
    [3]    1.5   1.5  -1.5   1.0  0.0
    [4]    0.0   0.0   0.0   0.0  0.0




