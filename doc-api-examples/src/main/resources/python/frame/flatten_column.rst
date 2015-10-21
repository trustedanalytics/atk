Examples
--------
Note that flatten_column has been deprecated.  Use flatten_columns() instead.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
<connect>

>>> my_frame = ta.Frame(ta.UploadRows([[1,"solo,mono,single"],
...                                 [2,"duo,double"]],
...                                 [('a',ta.int32),('b', str)]))
<progress>

</hide>
Given a data file::

    1-solo,mono,single
    2-duo,double

The commands to bring the data into a frame, where it can be worked on:

<skip>
.. only:: html

    .. code::

        >>> my_csv = ta.CsvFile("original_data.csv", schema=[('a', int32), ('b', str)], delimiter='-')
        >>> my_frame = ta.Frame(source=my_csv)

.. only:: latex

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32),
        ... ('b', str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

</skip>

Looking at it:

.. code::

    >>> my_frame.inspect()
    [#]  a  b
    ========================
    [0]  1  solo,mono,single
    [1]  2  duo,double


Now, spread out those sub-strings in column *b*:

.. code::

    >>> my_frame.flatten_column('b')
    -etc-

Check again:

.. code::

    >>> my_frame.inspect()
    [#]  a  b
    ==============
    [0]  1  solo
    [1]  1  mono
    [2]  1  single
    [3]  2  duo
    [4]  2  double


