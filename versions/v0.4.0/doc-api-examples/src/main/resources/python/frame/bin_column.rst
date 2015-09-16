Examples
--------
For this example, we will use a frame with column *a* accessed by a Frame
object *my_frame*:

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

Modify the frame with a column showing what bin the data is in.
The data values should use strict_binning:

.. code::

    >>> my_frame.bin_column('a', [5,12,25,60], include_lowest=True,
    ... strict_binning=True, bin_column_name='binned')
    >>> my_frame.inspect( n=11 )

      a:int32     binned:int32
    /---------------------------/
          1               -1
          1               -1
          2               -1
          3               -1
          5                0
          8                0
         13                1
         21                1
         34                2
         55                2
         89               -1

Modify the frame with a column showing what bin the data is in.
The data value should not use strict_binning:

.. code::

    >>> my_frame.bin_column('a', [5,12,25,60], include_lowest=True,
    ... strict_binning=False, bin_column_name='binned')
    >>> my_frame.inspect( n=11 )

      a:int32     binned:int32
    /---------------------------/
          1                0
          1                0
          2                0
          3                0
          5                0
          8                0
         13                1
         21                1
         34                2
         55                2
         89                2


Modify the frame with a column showing what bin the data is in.
The bins should be lower inclusive:

.. code::

    >>> my_frame.bin_column('a', [1,5,34,55,89], include_lowest=True,
    ... strict_binning=False, bin_column_name='binned')
    >>> my_frame.inspect( n=11 )

      a:int32     binned:int32
    /---------------------------/
          1                0
          1                0
          2                0
          3                0
          5                1
          8                1
         13                1
         21                1
         34                2
         55                3
         89                3

Modify the frame with a column showing what bin the data is in.
The bins should be upper inclusive:

.. code::

    >>> my_frame.bin_column('a', [1,5,34,55,89], include_lowest=False,
    ... strict_binning=True, bin_column_name='binned')
    >>> my_frame.inspect( n=11 )

      a:int32     binned:int32
    /---------------------------/
       1                   0
       1                   0
       2                   0
       3                   0
       5                   0
       8                   1
      13                   1
      21                   1
      34                   1
      55                   2
      89                   3
