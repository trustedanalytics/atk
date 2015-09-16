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
The data should be separated into a maximum of five bins and the bin cutoffs
should be evenly spaced.
Note that there may be bins with no members:

.. code::

    >>> cutoffs = my_frame.bin_column_equal_width('a', 5, 'aEWBinned')
    >>> my_frame.inspect( n=11 )

      a:int32     aEWBinned:int32
    /-----------------------------/
        1                 0
        1                 0
        2                 0
        3                 0
        5                 0
        8                 0
       13                 0
       21                 1
       34                 1
       55                 3
       89                 4

The method returns a list of 6 cutoff values that define the edges of each
bin.
Note that difference between the cutoff values is constant:

.. code::

    >>> print cutoffs
    [1.0, 18.6, 36.2, 53.8, 71.4, 89.0]
