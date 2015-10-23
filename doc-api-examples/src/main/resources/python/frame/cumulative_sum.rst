Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*:

.. code::

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

The cumulative sum for column *obs* is obtained by:

    >>> my_frame.cumulative_sum('obs')
    <progress>

The Frame *my_frame* accesses the original frame that now contains two
columns, *obs* that contains the original column values, and
*obsCumulativeSum* that contains the cumulative percent count:

    >>> my_frame.inspect()
    [#]  obs  obs_cumulative_sum
    ============================
    [0]    0                 0.0
    [1]    1                 1.0
    [2]    2                 3.0
    [3]    0                 3.0
    [4]    1                 4.0
    [5]    2                 6.0