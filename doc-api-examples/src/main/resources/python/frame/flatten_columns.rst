Examples
--------
<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
<connect>

>>> frame = ta.Frame(ta.UploadRows([[1,"solo,mono,single","green,yellow,red"],
...                                 [2,"duo,double","orange,black"]],
...                                 [('a',ta.int32),('b', str),('c', str)]))
<progress>

</hide>

Given a data file::

    1-solo,mono,single-green,yellow,red
    2-duo,double-orange,black

The commands to bring the data into a frame, where it can be worked on:

<skip>
.. only:: html

    .. code::

        >>> my_csv = ta.CsvFile("original_data.csv", schema=[('a', int32), ('b', str),('c',str)], delimiter='-')
        >>> frame = ta.Frame(source=my_csv)

.. only:: latex

    .. code::

        >>> my_csv = ta.CsvFile("original_data.csv", schema=[('a', int32),
        ... ('b', str),('c', str)], delimiter='-')
        >>> frame = ta.Frame(source=my_csv)

</skip>

Looking at it:

.. code::

    >>> frame.inspect()
    [#]  a  b                 c
    ==========================================
    [0]  1  solo,mono,single  green,yellow,red
    [1]  2  duo,double        orange,black

Now, spread out those sub-strings in column *b* and *c*:

.. code::

    >>> frame.flatten_columns(['b','c'], ',')
    <progress>

Note that the delimiters parameter is optional, and if no delimiter is specified, the default
is a comma (,).  So, in the above example, the delimiter parameter could be omitted.  Also, if
the delimiters are different for each column being flattened, a list of delimiters can be
provided.  If a single delimiter is provided, it's assumed that we are using the same delimiter
for all columns that are being flattened.  If more than one delimiter is provided, the number of
delimiters must match the number of string columns being flattened.

Check again:

.. code::

    >>> frame.inspect()
    [#]  a  b       c
    ======================
    [0]  1  solo    green
    [1]  1  mono    yellow
    [2]  1  single  red
    [3]  2  duo     orange
    [4]  2  double  black

<hide>
# Re-create frame with original data to start over with single column example
>>> ta.drop(frame)
1

>>> frame = ta.Frame(ta.UploadRows([[1,"solo,mono,single","green,yellow,red"],
...                                [2,"duo,double","orange,black"]],
...                                [('a',ta.int32),('b', str),('c', str)]))
<progress>

</hide>

Alternatively, flatten_columns also accepts a single column name (instead of a list) if just one
column is being flattened.  For example, we could have called flatten_column on just column *b*:


.. code::

    >>> frame.flatten_columns('b', ',')
    <progress>

Check again:

.. code ::

    >>> frame.inspect()
    [#]  a  b       c
    ================================
    [0]  1  solo    green,yellow,red
    [1]  1  mono    green,yellow,red
    [2]  1  single  green,yellow,red
    [3]  2  duo     orange,black
    [4]  2  double  orange,black



