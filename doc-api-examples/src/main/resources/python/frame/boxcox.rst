Examples
--------
Consider the following frame with column named 'input' storing numerical values

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

>>> frame = ta.Frame(ta.UploadRows([[7.7132064326674596],[0.207519493594015],[6.336482349262754],[7.4880388253861181],[4.9850701230259045]],[("input", ta.float64)]))
-etc-

</hide>

>>> frame.inspect()
[#]  input
===================
[0]   7.71320643267
[1]  0.207519493594
[2]   6.33648234926
[3]   7.48803882539
[4]   4.98507012303

We compute the box-cox transformation for the 'input' column with lambda 0.3
>>> frame.box_cox('input',0.3)
<progress>

The frame now contains an additional column 'input_lambda_0.3' with the box-cox transform for column 'input' with lambda 0.3
>>> frame.inspect()
[#]  input           input_lambda_0.3
=====================================
[0]   7.71320643267     2.81913279907
[1]  0.207519493594    -1.25365381375
[2]   6.33648234926     2.46673638752
[3]   7.48803882539     2.76469126003
[4]   4.98507012303     2.06401101556

We compute the reverse box-cox transformation for the 'input_lambda_0.3' column with lambda 0.3
>>> frame.reverse_box_cox('input_lambda_0.3', 0.3)
<progress>

The frame now contains an additional column 'input_lambda_0.3_reverse_lambda_0.3' with the reverse box-cox transform for
the column 'input_lambda_0.3' with lambda 0.3
>>> frame.inspect()
[#]  input           input_lambda_0.3  input_lambda_0.3_reverse_lambda_0.3
==========================================================================
[0]   7.71320643267     2.81913279907                        7.71320643267
[1]  0.207519493594    -1.25365381375                       0.207519493594
[2]   6.33648234926     2.46673638752                        6.33648234926
[3]   7.48803882539     2.76469126003                        7.48803882539
[4]   4.98507012303     2.06401101556                        4.98507012303


