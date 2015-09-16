Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*:

.. code::

    >>> cov = my_frame.covariance(['col_0', 'col_1'])
    >>> print(cov)

