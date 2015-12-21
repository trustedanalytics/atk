
Given a frame with data:

.. code::

    <hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    -etc-

    >>> frame = ta.Frame(ta.UploadRows([[200, 4, 25],
    ...                                 [200, 5, 25],
    ...                                 [200, 4, 25],
    ...                                 [200, 5, 35],
    ...                                 [200, 6, 25],
    ...                                 [200, 8, 35],
    ...                                 [200, 4, 45],
    ...                                 [200, 4, 25],
    ...                                 [200, 5, 25],
    ...                                 [201, 4, 25]],
    ...                                [("a", ta.int32),
    ...                                 ("b", ta.int32),
    ...                                 ("c", ta.int32)]))
    <progress>

    </hide>

    >>> frame.inspect()
    [#]  a    b  c
    ===============
    [0]  200  4  25
    [1]  200  5  25
    [2]  200  4  25
    [3]  200  5  35
    [4]  200  6  25
    [5]  200  8  35
    [6]  200  4  45
    [7]  200  4  25
    [8]  200  5  25
    [9]  201  4  25

Remove any rows that are identical to a previous row.
The result is a frame of unique rows.
Note that row order may change.

.. code::

    >>> frame.drop_duplicates()
    <progress>
    >>> frame.inspect()
    [#]  a    b  c
    ===============
    [0]  201  4  25
    [1]  200  4  25
    [2]  200  5  25
    [3]  200  8  35
    [4]  200  6  25
    [5]  200  5  35
    [6]  200  4  45


Now remove any rows that have the same data in columns *a* and
*c* as a previously checked row:

.. code::

    >>> frame.drop_duplicates([ "a", "c"])
    <progress>

The result is a frame with unique values for the combination of columns *a*
and *c*.

.. code::

    >>> frame.inspect()
    [#]  a    b  c
    ===============
    [0]  201  4  25
    [1]  200  4  45
    [2]  200  4  25
    [3]  200  8  35
