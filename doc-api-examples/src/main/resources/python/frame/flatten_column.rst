Examples
--------
Given a data file::

    1-"solo,mono,single"
    2-"duo,double"

The commands to bring the data into a frame, where it can be worked on:

.. only:: html

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32), ('b', str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

.. only:: latex

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32),
        ... ('b', str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

Looking at it:

.. code::

    >>> my_frame.inspect()

      a:int32   b:str
    /-------------------------------/
        1       solo, mono, single
        2       duo, double

Now, spread out those sub-strings in column *b*:

.. code::

    >>> my_frame.flatten_column('b')

Check again:

.. code::

    >>> my_frame.inspect()

      a:int32   b:str
    /------------------/
        1       solo
        1       mono
        1       single
        2       duo
        2       double

