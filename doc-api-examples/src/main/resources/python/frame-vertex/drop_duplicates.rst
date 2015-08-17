Examples
--------
Remove any rows that have the same data in column *b* as a previously
checked row:

.. code::

    >>> my_frame.drop_duplicates("b")

The result is a frame with unique values in column *b*.

Remove any rows that have the same data in columns *a* and *b* as a
previously checked row:

.. code::

    >>> my_frame.drop_duplicates([ "a", "b"] )

The result is a frame with unique values for the combination of columns
*a* and *b*.

Remove any rows that have the whole row identical:

.. code::

    >>> my_frame.drop_duplicates()

The result is a frame where something is different in every row from every
other row.
Each row is unique.

