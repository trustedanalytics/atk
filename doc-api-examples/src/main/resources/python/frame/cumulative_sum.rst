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

The cumulative sum for column *obs* is obtained by:

.. code::

    >>> my_frame.cumulative_sum('obs')

The Frame *my_frame* accesses the original frame that now contains two
columns, *obs* that contains the original column values, and
*obsCumulativeSum* that contains the cumulative percent count:

.. code::

    >>> my_frame.inspect()

      obs:int32   obs_cumulative_sum:int32
    /--------------------------------------/
         0                          0
         1                          1
         2                          3
         0                          3
         1                          4
         2                          6

