Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([[4.9,1.4,0], [4.7,1.3,0], [4.6,1.5,0], [6.3,4.9,1],
...                                 [6.1,4.7,1], [6.4,4.3,1], [6.6,4.4,1], [7.2,6.0,2],
...                                 [7.2,5.8,2], [7.4,6.1,2], [7.9,6.4,2]],
...                                 [('Sepal_Length', ta.float64),('Petal_Length', ta.float64), ('Class', int)]))
-etc-

</hide>
Consider the following frame containing three columns.

>>> frame.inspect()
[#]  Sepal_Length  Petal_Length  Class
======================================
[0]           4.9           1.4      0
[1]           4.7           1.3      0
[2]           4.6           1.5      0
[3]           6.3           4.9      1
[4]           6.1           4.7      1
[5]           6.4           4.3      1
[6]           6.6           4.4      1
[7]           7.2           6.0      2
[8]           7.2           5.8      2
[9]           7.4           6.1      2

>>> model = ta.LogisticRegressionModel()
<progress>
>>> train_output = model.train(frame, 'Class', ['Sepal_Length', 'Petal_Length'],
...                                 num_classes=3, optimizer='LBFGS', compute_covariance=True)
<progress>
<skip>
>>> train_output.summary_table
                coefficients  degrees_freedom  standard_errors  \
intercept_0        -0.780153                1              NaN
Sepal_Length_1   -120.442165                1  28497036.888425
Sepal_Length_0    -63.683819                1  28504715.870243
intercept_1       -90.484405                1              NaN
Petal_Length_0    117.979824                1  36178481.415888
Petal_Length_1    206.339649                1  36172481.900910

                wald_statistic   p_value
intercept_0                NaN       NaN
Sepal_Length_1       -0.000004  1.000000
Sepal_Length_0       -0.000002  1.000000
intercept_1                NaN       NaN
Petal_Length_0        0.000003  0.998559
Petal_Length_1        0.000006  0.998094

>>> train_output.covariance_matrix.inspect()
[#]  Sepal_Length_0      Petal_Length_0      intercept_0
===============================================================
[0]   8.12518826843e+14   -1050552809704907   5.66008788624e+14
[1]  -1.05055305606e+15   1.30888251756e+15   -3.5175956714e+14
[2]   5.66010683868e+14  -3.51761845892e+14  -2.52746479908e+15
[3]   8.12299962335e+14  -1.05039425964e+15   5.66614798332e+14
[4]  -1.05027789037e+15    1308665462990595    -352436215869081
[5]     566011198950063  -3.51665950639e+14   -2527929411221601

[#]  Sepal_Length_1      Petal_Length_1      intercept_1
===============================================================
[0]     812299962806401  -1.05027764456e+15   5.66009303434e+14
[1]  -1.05039450654e+15   1.30866546361e+15  -3.51663671537e+14
[2]     566616693386615   -3.5243849435e+14   -2.5279294114e+15
[3]    8.1208111142e+14   -1050119118230513   5.66615352448e+14
[4]  -1.05011936458e+15   1.30844844687e+15   -3.5234036349e+14
[5]     566617247774244  -3.52342642321e+14   -2528394057347494
</skip>

>>> predicted_frame = model.predict(frame, ['Sepal_Length', 'Petal_Length'])
<progress>
>>> predicted_frame.inspect()
[#]  Sepal_Length  Petal_Length  Class  predicted_label
=======================================================
[0]           4.9           1.4      0                0
[1]           4.7           1.3      0                0
[2]           4.6           1.5      0                0
[3]           6.3           4.9      1                1
[4]           6.1           4.7      1                1
[5]           6.4           4.3      1                1
[6]           6.6           4.4      1                1
[7]           7.2           6.0      2                2
[8]           7.2           5.8      2                2
[9]           7.4           6.1      2                2

>>> test_metrics = model.test(frame, 'Class', ['Sepal_Length', 'Petal_Length'])
<progress>
>>> test_metrics
Precision: 1.0
Recall: 1.0
Accuracy: 1.0
FMeasure: 1.0
Confusion Matrix:
            Predicted_0.0  Predicted_1.0  Predicted_2.0
Actual_0.0              3              0              0
Actual_1.0              0              4              0
Actual_2.0              0              0              4