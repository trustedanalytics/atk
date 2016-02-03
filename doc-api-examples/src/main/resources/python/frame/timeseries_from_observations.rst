Examples
--------
In this example, we will use a frame of observations of resting heart rate for
three individuals over three days.  The data is accessed from Frame object
called *my_frame*:

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

>>> data = [["Edward", "2016-01-01T12:00:00Z", 62],["Sarah", "2016-01-01T12:00:00Z", 65],["Stanley", "2016-01-01T12:00:00Z", 57],["Edward", "2016-01-02T12:00:00Z", 63],["Sarah", "2016-01-02T12:00:00Z", 64],["Stanley", "2016-01-02T12:00:00Z", 57],["Edward", "2016-01-03T12:00:00Z", 62],["Sarah", "2016-01-03T12:00:00Z", 64],["Stanley", "2016-01-03T12:00:00Z", 56]]
>>> schema = [("name", str), ("date", ta.datetime), ("resting_heart_rate", ta.float64)]

>>> my_frame = ta.Frame(ta.UploadRows(data, schema))
-etc-

</hide>

.. code::

 >>> my_frame.inspect( my_frame.row_count )
 [#]  name     date                      resting_heart_rate
 ==========================================================
 [0]  Edward   2016-01-01T12:00:00.000Z                62.0
 [1]  Sarah    2016-01-01T12:00:00.000Z                65.0
 [2]  Stanley  2016-01-01T12:00:00.000Z                57.0
 [3]  Edward   2016-01-02T12:00:00.000Z                63.0
 [4]  Sarah    2016-01-02T12:00:00.000Z                64.0
 [5]  Stanley  2016-01-02T12:00:00.000Z                57.0
 [6]  Edward   2016-01-03T12:00:00.000Z                62.0
 [7]  Sarah    2016-01-03T12:00:00.000Z                64.0
 [8]  Stanley  2016-01-03T12:00:00.000Z                56.0


We then need to create an array that contains the date/time index,
which will be used when creating the time series.  Since our data
is for three days, our date/time index will just contain those
three dates:

.. code::

 >>> datetimeindex = ["2016-01-01T12:00:00.000Z","2016-01-02T12:00:00.000Z","2016-01-03T12:00:00.000Z"]

Then we can create our time series frame by specifying our date/time
index along with the name of our timestamp column (in this example, it's
 "date"), key column (in this example, it's "name"), and value column (in
this example, it's "resting_heart_rate").

.. code::

 >>> ts = my_frame.timeseries_from_observations(datetimeindex, "date", "name", "resting_heart_rate")
 <progress>

Take a look at the resulting time series frame schema and contents:

.. code::

 >>> ts.schema
 [(u'name', <type 'unicode'>), (u'resting_heart_rate', vector(3))]

 >>> ts.inspect()
 [#]  name     resting_heart_rate
 ================================
 [0]  Stanley  [57.0, 57.0, 56.0]
 [1]  Edward   [62.0, 63.0, 62.0]
 [2]  Sarah    [65.0, 64.0, 64.0]

