Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
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

The cumulative percent count for column *obs* is obtained by:

.. code::

    >>> my_frame.tally_percent('obs', 1)

The Frame *my_frame* accesses the original frame that now contains two
columns, *obs* that contains the original column values, and
*obsCumulativePercentCount* that contains the cumulative percent count:

.. code::

    >>> my_frame.inspect()

      obs:int32    obs_tally_percent:float64
    /----------------------------------------/
         0                         0.0
         1                         0.5
         2                         0.5
         0                         0.5
         1                         1.0
         2                         1.0

