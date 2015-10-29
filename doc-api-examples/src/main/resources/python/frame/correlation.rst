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


my_frame.correlation computes the common correlation coefficient (Pearson's) on the pair
of columns provided.
In this example, the *idnum* and most of the columns have trivial correlations: -1, 0, or +1.
Column *x3* provides a contrasting coefficient of 3 / sqrt(3) = 0.948683298051 .


    >>> my_frame.correlation(["x1", "x2"])
    <progress>

        -1.0
    >>> my_frame.correlation(["x1", "x4"])
    <progress>

        0.0
    >>> my_frame.correlation(["x2", "x3"])
    <progress>

        -0.948683298051


