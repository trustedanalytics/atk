Examples
--------
Consider Frame *my_frame*, which contains the data

    <hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-

    >>> s = [('a', str),('b', ta.int32),('labels', ta.int32),('predictions', ta.int32)]
    >>> rows = [ ["red", 1, 0, 0], ["blue", 3, 1, 0],["green", 1, 0, 0],["green", 0, 1, 1]]
    >>> my_frame = ta.Frame(ta.UploadRows (rows, s))
    -etc-

    </hide>
    >>> my_frame.inspect()
    [#]  a      b  labels  predictions
    ==================================
    [0]  red    1       0            0
    [1]  blue   3       1            0
    [2]  green  1       0            0
    [3]  green  0       1            1


    >>> cm = my_frame.classification_metrics('labels', 'predictions', 1, 1)
    <progress>

    >>> cm.f_measure
    0.6666666666666666

    >>> cm.recall
    0.5

    >>> cm.accuracy
    0.75

    >>> cm.precision
    1.0

    >>> cm.confusion_matrix
                Predicted_Pos  Predicted_Neg
    Actual_Pos              1              1
    Actual_Neg              0              2



