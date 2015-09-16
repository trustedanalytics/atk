Examples
--------
Given a frame with column *a* accessed by a Frame object *my_frame*:

.. code::

    >>> my_frame.inspect( n=11 )

      a:int32
    /---------/
        1
        1
        2
        3
        5
        8
       13
       21
       34
       55
       89

Modify the frame, adding a column showing what bin the data is in.
The data should be grouped into a maximum of five bins.
Note that each bin will have the same quantity of members (as much as
possible):

.. code::

    >>> cutoffs = my_frame.bin_column_equal_depth('a', 5, 'aEDBinned')
    >>> my_frame.inspect( n=11 )

      a:int32     aEDBinned:int32
    /-----------------------------/
          1                   0
          1                   0
          2                   1
          3                   1
          5                   2
          8                   2
         13                   3
         21                   3
         34                   4
         55                   4
         89                   4

    >>> print cutoffs
    [1.0, 2.0, 5.0, 13.0, 34.0, 89.0]
