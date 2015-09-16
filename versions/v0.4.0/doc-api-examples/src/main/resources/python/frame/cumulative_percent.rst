Examples
--------
Consider Frame *my_frame* accessing a frame that contains a single
column named *obs*:

.. code::

    >>> my_frame.inspect()

      obs:int32
    /-----------/
         0
         1
         2
         0
         1
         2

The cumulative percent sum for column *obs* is obtained by:

.. code::

    >>> my_frame.cumulative_percent('obs')

The Frame *my_frame* now contains two columns *obs* and
*obsCumulativePercentSum*.
They contain the original data and the cumulative percent sum,
respectively:

.. code::

    >>> my_frame.inspect()

      obs:int32   obs_cumulative_percent:float64
    /--------------------------------------------/
         0                             0.0
         1                             0.16666666
         2                             0.5
         0                             0.5
         1                             0.66666666
         2                             1.0

