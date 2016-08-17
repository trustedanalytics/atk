Examples
--------

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> data = [["2016-04-29T08:00:00.000Z",50,1.0,30.36],
...         ["2016-05-02T08:00:00.000Z",-50,2.1,30.61],
...         ["2016-05-03T08:00:00.000Z",50,3.0,30.36],
...         ["2016-05-04T08:00:00.000Z",-50,3.9,29.85],
...         ["2016-05-05T08:00:00.000Z",50,4.8,29.90],
...         ["2016-05-06T08:00:00.000Z",-50,6.0,30.04],
...         ["2016-05-09T08:00:00.000Z",50,7.2,29.80],
...         ["2016-05-10T08:00:00.000Z",-50,8.0,30.14],
...         ["2016-05-11T08:00:00.000Z",50,9.1,30.06],
...         ["2016-05-12T08:00:00.000Z",-50,10.2,29.76],
...         ["2016-05-13T08:00:00.000Z",50,9.3,29.91],
...         ["2016-05-16T08:00:00.000Z",-50,7.9,30.39],
...         ["2016-05-17T08:00:00.000Z",50,7.0,29.98],
...         ["2016-05-18T08:00:00.000Z",-50,6.0,29.99],
...         ["2016-05-19T08:00:00.000Z",50,4.9,29.63],
...         ["2016-05-20T08:00:00.000Z",-50,4.1,30.15],
...         ["2016-05-23T08:00:00.000Z",50,3.0,31.23],
...         ["2016-05-24T08:00:00.000Z",-50,2.1,31.06],
...         ["2016-05-25T08:00:00.000Z",50,0.9,31.39]]
>>> schema = [("date", ta.datetime), ("a", ta.int32), ("b", ta.float32), ("c", ta.float32)]
>>> frame = ta.Frame(ta.UploadRows(data, schema))
-etc-
</hide>

In this example, we have a frame that contains time series values.  The inspect command below shows a snippet of
what the data looks like:

>>> frame.inspect()
[#]  date                      a    b              c
================================================================
[0]  2016-04-29T08:00:00.000Z   50            1.0  30.3600006104
[1]  2016-05-02T08:00:00.000Z  -50  2.09999990463  30.6100006104
[2]  2016-05-03T08:00:00.000Z   50            3.0  30.3600006104
[3]  2016-05-04T08:00:00.000Z  -50  3.90000009537  29.8500003815
[4]  2016-05-05T08:00:00.000Z   50  4.80000019073  29.8999996185
[5]  2016-05-06T08:00:00.000Z  -50            6.0  30.0400009155
[6]  2016-05-09T08:00:00.000Z   50  7.19999980927  29.7999992371
[7]  2016-05-10T08:00:00.000Z  -50            8.0  30.1399993896
[8]  2016-05-11T08:00:00.000Z   50  9.10000038147  30.0599994659
[9]  2016-05-12T08:00:00.000Z  -50  10.1999998093  29.7600002289


Perform the augmented Dickey-Fuller test by specifying the name of the column that contains the time series values, the
max lag, and optionally the method of regression (using MacKinnon's notation).  If no regression method is specified,
it will default constant ("c").

Calcuate the augmented Dickey-Fuller test statistic for column "b" with no lag:

>>> result = frame.timeseries_augmented_dickey_fuller_test("b", 0)
<progress>

>>> result["p_value"]
0.7317795217142998

>>> result["test_stat"]
-1.0573441288025922