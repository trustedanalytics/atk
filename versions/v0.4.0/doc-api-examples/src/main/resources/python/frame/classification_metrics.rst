Examples
--------
Consider the following sample data set in *frame* with actual data
labels specified in the *labels* column and the predicted labels in the
*predictions* column:

.. code::

    >>> frame.inspect()

      a:unicode   b:int32   labels:int32  predictions:int32
    /-------------------------------------------------------/
        red         1              0                  0
        blue        3              1                  0
        blue        1              0                  0
        green       0              1                  1

    >>> cm = frame.classification_metrics(label_column='labels',
    ... pred_column='predictions', pos_label=1, beta=1)

    >>> cm.f_measure

    0.66666666666666663

    >>> cm.recall

    0.5

    >>> cm.accuracy

    0.75

    >>> cm.precision

    1.0

    >>> cm.confusion_matrix

                  Predicted
                 _pos_ _neg__
    Actual  pos |  1     1
            neg |  0     2

