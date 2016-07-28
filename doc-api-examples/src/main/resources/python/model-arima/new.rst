
Consider the following frame of observations collected over seven days.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

>>> frame = ta.Frame(ta.UploadRows([["2015-01-01T00:00:00.000Z","Sarah",12.88969427],["2015-01-02T00:00:00.000Z","Sarah",13.54964408],
...                                 ["2015-01-03T00:00:00.000Z","Sarah",13.8432745],["2015-01-04T00:00:00.000Z","Sarah",12.13843611],
...                                 ["2015-01-05T00:00:00.000Z","Sarah",12.81156092],["2015-01-06T00:00:00.000Z","Sarah",14.2499628],
...                                 ["2015-01-07T00:00:00.000Z","Sarah",15.12102595]],
...                                 [("timestamp", ta.datetime),("name", str),("value", ta.float64)]))
-etc-

</hide>
The frame has three columns: timestamp, name, and value.

>>> frame.inspect()
[#]  timestamp                 name   value
=================================================
[0]  2015-01-01T00:00:00.000Z  Sarah  12.88969427
[1]  2015-01-02T00:00:00.000Z  Sarah  13.54964408
[2]  2015-01-03T00:00:00.000Z  Sarah   13.8432745
[3]  2015-01-04T00:00:00.000Z  Sarah  12.13843611
[4]  2015-01-05T00:00:00.000Z  Sarah  12.81156092
[5]  2015-01-06T00:00:00.000Z  Sarah   14.2499628
[6]  2015-01-07T00:00:00.000Z  Sarah  15.12102595



Define the date time index:

>>> datetimeindex = ['2015-01-01T00:00:00.000Z','2015-01-02T00:00:00.000Z',
... '2015-01-03T00:00:00.000Z','2015-01-04T00:00:00.000Z','2015-01-05T00:00:00.000Z',
... '2015-01-06T00:00:00.000Z','2015-01-07T00:00:00.000Z']

Then, create a time series frame from the frame of observations, since the ARIMA model
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


Use the frame take function to get one row of data with just the "value" column

>>> ts_frame_data = ts.take(n=1,offset=0,columns=["value"])

From the ts_frame_data, get the first row and first column to extract out just the time series values.

>>> ts_values = ts_frame_data[0][0]

>>> ts_values
[12.88969427,
 13.54964408,
 13.8432745,
 12.13843611,
 12.81156092,
 14.2499628,
 15.12102595]

Create an ARIMA model:

>>> model = ta.ArimaModel()
<progress>

Train the model using the timeseries frame:

>>> model.train(ts_values, 1, 0, 1)
<progress>
{u'coefficients': [9.864444620964322, 0.2848511106449633, 0.47346114378593795]}

Call predict to forecast values by passing the number of future periods to predict beyond the length of the time series.
Since the parameter in this example is 0, predict will forecast 7 values (the same number of values that were in the
original time series vector).

>>> model.predict(0)
<progress>
{u'forecasted': [12.674342627141744,
  13.638048984791693,
  13.682219498657313,
  13.883970022400577,
  12.49564914570843,
  13.66340392811346,
  14.201275185574925]}

>>> model.publish()
<progress>

Take the path to the published model and run it in the Scoring Engine:

<skip>
>>> import requests
>>> headers = {'Content-type': 'application/json', 'Accept': 'application/json,text/plain'}
</skip>

Post a request to get the metadata about the model.

<skip>
>>> r = requests.get('http://mymodel.demotrustedanalytics.com/v2/metadata')
>>> r.text
u'{"model_details":{"model_type":"ARIMA Model","model_class":"com.cloudera.sparkts.models.ARIMAModel","model_reader":"org.trustedanalytics.atk.scoring.models.ARIMAModelReaderPlugin","custom_values":{}},"input":[{"name":"timeseries","value":"Array[Double]"},{"name":"future","value":"Int"}],"output":[{"name":"timeseries","value":"Array[Double]"},{"name":"future","value":"Int"},{"name":"predicted_values","value":"Array[Double]"}]}'
</skip>

ARIMA model support started in version 2 of the scoring engine REST API. We send the number of values to forecast
beyond the length of the time series (in this example we are passing 0). This means that since 7 historical time series
values were provided, 7 future periods will be forecasted.

<skip>
>>> r = requests.post('http://mymodel.demotrustedanalytics.com/v2/score',json={"records":[{"future":0}]})
</skip>

The 'predicted_values' array contains the future values, which have been forecasted based on the historical data.

<skip>
>>> r.text
u'{"data":[{"future":0.0,"predicted_values":[12.674342627141744,13.638048984791693,13.682219498657313,13.883970022400577,12.49564914570843,13.66340392811346,14.201275185574925]}]}'
</skip>
