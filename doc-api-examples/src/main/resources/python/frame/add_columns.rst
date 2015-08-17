Examples
--------

ven a Frame *my_frame* identifying a data frame with two int32
columns *column1* and *column2*.
Add a third column *column3* as an int32 and fill it with the
contents of *column1* and *column2* multiplied together:

.. code::

    >>> my_frame.add_columns(lambda row: row.column1*row.column2,
    ... ('column3', int32))

The frame now has three columns, *column1*, *column2* and *column3*.
The type of *column3* is an int32, and the value is the product of
*column1* and *column2*.

Add a string column *column4* that is empty:

.. code::

    >>> my_frame.add_columns(lambda row: '', ('column4', str))

The Frame object *my_frame* now has four columns *column1*, *column2*,
*column3*, and *column4*.
The first three columns are int32 and the fourth column is str.
Column *column4* has an empty string ('') in every row.

Multiple columns can be added at the same time.
Add a column *a_times_b* and fill it with the contents of column *a*
multiplied by the contents of column *b*.
At the same time, add a column *a_plus_b* and fill it with the contents
of column *a* plus the contents of column *b*:

.. only:: html

    .. code::

        >>> my_frame.add_columns(lambda row: [row.a * row.b, row.a + row.b], [("a_times_b", float32), ("a_plus_b", float32)])

.. only:: latex

    .. code::

        >>> my_frame.add_columns(lambda row: [row.a * row.b, row.a +
        ... row.b], [("a_times_b", float32), ("a_plus_b", float32)])

Two new columns are created, "a_times_b" and "a_plus_b", with the
appropriate contents.

Given a frame of data and Frame *my_frame* points to it.
In addition we have defined a |UDF| *func*.
Run *func* on each row of the frame and put the result in a new int
column *calculated_a*:

.. code::

    >>> my_frame.add_columns( func, ("calculated_a", int))

Now the frame has a column *calculated_a* which has been filled with
the results of the |UDF| *func*.

A |UDF| must return a value in the same format as the column is
defined.
In most cases this is automatically the case, but sometimes it is less
obvious.
Given a |UDF| *function_b* which returns a value in a list, store
the result in a new column *calculated_b*:

.. code::

    >>> my_frame.add_columns(function_b, ("calculated_b", float32))

This would result in an error because function_b is returning a value
as a single element list like [2.4], but our column is defined as a
tuple.
The column must be defined as a list:

.. code::

    >>> my_frame.add_columns(function_b, [("calculated_b", float32)])

To run an optimized version of add_columns, columns_accessed parameter can
be populated with the column names which are being accessed in |UDF|. This
speeds up the execution by working on only the limited feature set than the
entire row.

Let's say a frame has 4 columns named *a*,*b*,*c* and *d* and we want to add a new column
with value from column *a* multiplied by value in column *b* and call it *a_times_b*.
In the example below, columns_accessed is a list with column names *a* and *b*.

.. code::

    >>> my_frame.add_columns(lambda row: row.a * row.b, ("a_times_b", float32), columns_accessed=["a", "b"])

add_columns would fail if columns_accessed parameter is not populated with the correct list of accessed
columns. If not specified, columns_accessed defaults to None which implies that all columns might be accessed
by the |UDF|.

More information on a row |UDF| can be found at :doc:`/ds_apir`.
