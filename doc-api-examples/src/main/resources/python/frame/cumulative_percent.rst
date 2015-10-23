Examples
--------
Consider Frame *my_frame* accessing a frame that contains a single
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

The cumulative percent sum for column *obs* is obtained by:

    >>> my_frame.cumulative_percent('obs')
    <progress>

The Frame *my_frame* now contains two columns *obs* and
*obsCumulativePercentSum*.
They contain the original data and the cumulative percent sum,
respectively:

    >>> my_frame.inspect()
    [#]  obs  obs_cumulative_percent
    ================================
    [0]    0                     0.0
    [1]    1          0.166666666667
    [2]    2                     0.5
    [3]    0                     0.5
    [4]    1          0.666666666667
    [5]    2                     1.0
