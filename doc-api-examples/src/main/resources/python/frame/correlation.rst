Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*:

.. code::

    >>> corr = my_frame.correlation(['col_0', 'col_1'])
    >>> print(corr)

