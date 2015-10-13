Examples
--------
Given a data file::

    1-solo,mono,single-green,yellow,red
    2-duo,double-orange,black

The commands to bring the data into a frame, where it can be worked on:

.. only:: html

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32), ('b', str),('c',str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

.. only:: latex

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32),
        ... ('b', str),('c', str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

Looking at it:

.. code::

    >>> my_frame.inspect()

    [#]  a  b                 c
    ==========================================
    [0]  1  solo,mono,single  green,yellow,red
    [1]  2  duo,double        orange,black


Now, spread out those sub-strings in column *b* and *c*:

.. code::

    >>> my_frame.flatten_columns(['b','c'], ',')

Note that the delimiters parameter is optional, and if no delimiter is specified, the default
is a comma (,).  So, in the above example, the delimiter parameter could be omitted.  Also, if
the delimiters are different for each column being flattened, a list of delimiters can be
provided.  If a single delimiter is provided, it's assumed that we are using the same delimiter
for all columns that are being flattened.

Check again:

.. code::

    >>> my_frame.inspect()

    [#]  a  b       c
    ======================
    [0]  1  solo    green
    [1]  1  mono    yellow
    [2]  1  single  red
    [3]  2  duo     orange
    [4]  2  double  black



