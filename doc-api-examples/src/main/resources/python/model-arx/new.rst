
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> schema = [("y", ta.float64),("visitors", ta.float64),("wkends", ta.float64),("incidentRate", ta.float64),("seasonality", ta.float64)]
>>> frame = ta.Frame(ta.UploadRows([[68,278,0,28,0.015132758079119],
                                    [89,324,0,28,0.0115112433251418],
                                    [96,318,0,28,0.0190129524583803],
                                    [98,347,0,28,0.0292307976571017],
                                    [70,345,1,28,0.0232811662755677],
                                    [88,335,1,29,0.0306535355961641],
                                    [76,309,0,29,0.0278080597180392],
                                    [104,318,0,29,0.0305241957835221],
                                    [64,308,0,29,0.0247039042146302],
                                    [89,320,0,29,0.0269026810295449],
                                    [76,292,0,29,0.0283254189686074],
                                    [66,295,1,29,0.0230224866502836],
                                    [84,383,1,21,0.0279373995306813],
                                    [49,237,0,21,0.0263853217789767],
                                    [47,210,0,21,0.0230224866502836]],
                                    schema=schema))
-etc-

</hide>
Consider the following frame containing five columns where "y" is the time series value and "vistors", "wkends",
"incidentRate", and "seasonality" are exogenous inputs.

>>> frame.inspect()
[#]  y     visitors  wkends  incidentRate  seasonality
==========================================================
[0]  68.0     278.0     0.0          28.0  0.0151327580791
[1]  89.0     324.0     0.0          28.0  0.0115112433251
[2]  96.0     318.0     0.0          28.0  0.0190129524584
[3]  98.0     347.0     0.0          28.0  0.0292307976571
[4]  70.0     345.0     1.0          28.0  0.0232811662756


>>> model = ta.ArxModel()
<progress>

>>> train_output = model.train(frame, "y", ["visitors", "wkends", "incidentRate", "seasonality"], 0, 0, True)
<progress>