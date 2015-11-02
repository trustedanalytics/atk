Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*:

    <hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-

    >>> s = [("obs", ta.int32)]
    >>> rows = [ [0], [1], [2], [0], [1], [2]]
    >>> my_frame = ta.Frame(ta.UploadRows (rows, s))
    -etc-

    </hide>
    >>> my_frame.inspect()
    [#]  obs
    ========
    [0]    0
    [1]    1
    [2]    2
    [3]    0
    [4]    1
    [5]    2

The cumulative percent count for column *obs* is obtained by:

    >>> my_frame.tally_percent("obs", "1")
    <progress>

The Frame *my_frame* accesses the original frame that now contains two
columns, *obs* that contains the original column values, and
*obsCumulativePercentCount* that contains the cumulative percent count:

    >>> my_frame.inspect()
    [#]  obs  obs_tally_percent
    ===========================
    [0]    0                0.0
    [1]    1                0.5
    [2]    2                0.5
    [3]    0                0.5
    [4]    1                1.0
    [5]    2                1.0

