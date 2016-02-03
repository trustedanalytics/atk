Examples
--------
For this example, we start with a frame that has already been formatted as a time series.
This means that the frame has a string column for key and a vector column that contains
a series of the observed values.  We must also know the date/time index that corresponds

The time series is in a Frame object called *ts_frame*.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-


>>> schema= [("key", str), ("series", ta.vector(6))]
>>> data = [["A", [62,55,60,61,60,59]],["B", [60,58,61,62,60,61]],["C", [69,68,68,70,71,69]]]

>>> ts_frame = ta.Frame(ta.UploadRows(data, schema))
-etc-

</hide>

.. code::

    >>> ts_frame.inspect()
    [#]  key  series
    ==============================================
    [0]  A    [62.0, 55.0, 60.0, 61.0, 60.0, 59.0]
    [1]  B    [60.0, 58.0, 61.0, 62.0, 60.0, 61.0]
    [2]  C    [69.0, 68.0, 68.0, 70.0, 71.0, 69.0]

Next, we define the date/time index.  In this example, it is one day intervals from
2016-01-01 to 2016-01-06:

.. code::

    >>> datetimeindex = ["2016-01-01T12:00:00.000Z","2016-01-02T12:00:00.000Z","2016-01-03T12:00:00.000Z","2016-01-04T12:00:00.000Z","2016-01-05T12:00:00.000Z","2016-01-06T12:00:00.000Z"]

Get a slice of our time series from 2016-01-02 to 2016-01-04:

.. code::
    >>> slice_start = "2016-01-02T12:00:00.000Z"
    >>> slice_end = "2016-01-04T12:00:00.000Z"

    >>> sliced_frame = ts_frame.timeseries_slice(datetimeindex, slice_start, slice_end)
    <progress>

Take a look at our sliced time series:

.. code::

    >>> sliced_frame.inspect()
    [#]  key  series
    ============================
    [0]  A    [55.0, 60.0, 61.0]
    [1]  B    [58.0, 61.0, 62.0]
    [2]  C    [68.0, 68.0, 70.0]
