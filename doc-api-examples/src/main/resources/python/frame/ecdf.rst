Examples
--------
Consider the following sample data set in *frame* with actual data labels
specified in the *labels* column and the predicted labels in the
*predictions* column:

.. code::

    >>> import trustedanalytics as ta
    >>> import pandas as p
    >>> f = ta.Frame(ta.Pandas(p.DataFrame([1, 3, 1, 0]), [('numbers', ta.int32)]))

    [==Job Progress...]

    >>> f.take(5)
    [[1], [3], [1], [0]]

    [==Job Progress...]

    >>> result = f.ecdf('numbers')
    >>> result.inspect()

      b:int32   b_ECDF:float64
    /--------------------------/
       1             0.2
       2             0.5
       3             0.8
       4             1.0

