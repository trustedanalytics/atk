
Consider the following frame of observations collected over the course of five days.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

>>> frame = ta.Frame(ta.UploadRows([["2015-01-01","Sarah",12.88969427],["2015-01-02","Sarah",13.54964408],
...                                 ["2015-01-03","Sarah",13.8432745],["2015-01-04","Sarah",12.13843611],
...                                 ["2015-01-05","Sarah",12.81156092],["2015-01-06","Sarah",14.2499628],
...                                 ["2015-01-07","Sarah",15.12102595]],
...                                 [("timestamp", ta.datetime),("name", str),("value", ta.float64)]))
-etc-

</hide>
The frame has three columns: timestamp, name, and value.

>>> frame.inspect()
[#]  timestamp                      name   value
======================================================
[0]  2015-01-01T00:00:00.000-08:00  Sarah  12.88969427
[1]  2015-01-02T00:00:00.000-08:00  Sarah  13.54964408
[2]  2015-01-03T00:00:00.000-08:00  Sarah   13.8432745
[3]  2015-01-04T00:00:00.000-08:00  Sarah  12.13843611
[4]  2015-01-05T00:00:00.000-08:00  Sarah  12.81156092
[5]  2015-01-06T00:00:00.000-08:00  Sarah   14.2499628
[6]  2015-01-07T00:00:00.000-08:00  Sarah  15.12102595


Define the date time index:

>>> datetimeindex = ['2015-01-01T00:00:00.000-08:00','2015-01-02T00:00:00.000-08:00',
... '2015-01-03T00:00:00.000-08:00','2015-01-04T00:00:00.000-08:00','2015-01-05T00:00:00.000-08:00',
... '2015-01-06T00:00:00.000-08:00','2015-01-07T00:00:00.000-08:00']

Then create a time series frame from the frame of observations, since the ARIMA model
expects data to be in a time series format (where the time series values are in a
vector column).

>>> ts = frame.timeseries_from_observations(datetimeindex, "timestamp","name","value")
<progress>

>>> ts.inspect()
[#]  name
==========
[0]  Sarah
<BLANKLINE>
[#]  value
================================================================================
[0]  [12.88969427, 13.54964408, 13.8432745, 12.13843611, 12.81156092, 14.2499628, 15.12102595]


Create an ARIMA model:

>>> model = ta.ArimaModel()
<progress>

Train the model using the timeseries frame:

>>> model.train(ts, "value", 1, 0, 1)
<progress>
{u'coefficients': [9.864444620964322, 0.2848511106449633, 0.47346114378593795]}

Predict future periods:

>>> predicted = model.predict(ts, 0, "value")
<progress>

The predicted frame returned has the same columns that were in the time series frame
provided, plus an added column called "predicted_values" that contains a vector of
forecasted values.

>>> predicted.column_names
[u'name', u'value', u'predicted_values']

Take a look at the vector of predicted values:

>>> predicted.inspect(columns="predicted_values")
[#]  predicted_values
================================================================================
[0]  [12.674342627141744, 13.638048984791693, 13.682219498657313, 13.883970022400577, 12.49564914570843, 13.66340392811346, 14.201275185574925]



