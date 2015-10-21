Examples
--------
Note that unflatten_column has been deprecated.  Use unflatten_columns() instead.
<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
<connect>

>>> frame = ta.Frame(ta.UploadRows([["user1", "1/1/2015", 1, 70],
...                                 ["user1", "1/1/2015", 2, 60],
...                                 ["user2", "1/1/2015", 1, 65]],
...                                [('a', str), ('b', str),('c', ta.int32) ,('d', ta.int32)]))
<progress>

</hide>
Given a data file::

    user1 1/1/2015 1 70
    user1 1/1/2015 2 60
    user2 1/1/2015 1 65

The commands to bring the data into a frame, where it can be worked on:

<skip>
.. only:: html

    .. code::

        >>> my_csv = ta.CsvFile("original_data.csv", schema=[('a', str), ('b', str),('c', int32) ,('d', int32)])
        >>> frame = ta.Frame(source=my_csv)

.. only:: latex

    .. code::

        >>> my_csv = ta.CsvFile("unflatten_column.csv", schema=[('a', str), ('b', str),('c', int32) ,('d', int32)])
        >>> frame = ta.Frame(source=my_csv)

</skip>
Looking at it:

.. code::

    >>> frame.inspect()
    [#]  a      b         c  d
    ===========================
    [0]  user1  1/1/2015  1  70
    [1]  user1  1/1/2015  2  60
    [2]  user2  1/1/2015  1  65


Unflatten the data using columns a & b:

.. code::

    >>> frame.unflatten_column(['a','b'])
    -etc-

Check again:

.. code::

    >>> frame.inspect()
    [#]  a      b         c    d
    ================================
    [0]  user2  1/1/2015  1    65
    [1]  user1  1/1/2015  1,2  70,60

Alternatively, unflatten_column() also accepts a single column like:

<hide>
# Re-create frame with original data to start over with single column example
>>> ta.drop(frame)
1

>>> frame = ta.Frame(ta.UploadRows([["user1", "1/1/2015", 1, 70],
...                                 ["user1", "1/1/2015", 2, 60],
...                                 ["user2", "1/1/2015", 1, 65]],
...                                [('a', str), ('b', str),('c', ta.int32) ,('d', ta.int32)]))
<progress>


</hide>

.. code::

    >>> frame.unflatten_column('a')
    -etc-

    >>> frame.inspect()
    [#]  a      b                  c    d
    =========================================
    [0]  user1  1/1/2015,1/1/2015  1,2  70,60
    [1]  user2  1/1/2015           1    65
