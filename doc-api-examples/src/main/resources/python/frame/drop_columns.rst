Examples
--------
For this example, Frame object *my_frame* accesses a frame with
columns *column_a*, *column_b*, *column_c* and *column_d*.

.. only:: html

    .. code::

        >>> print my_frame.schema
        [("column_a", str), ("column_b", atk.int32), ("column_c", str), ("column_d", atk.int32)]

.. only:: latex

    .. code::

        >>> print my_frame.schema
        [("column_a", str), ("column_b", atk.int32), ("column_c", str),
        ("column_d", atk.int32)]

Eliminate columns *column_b* and *column_d*:

.. code::

    >>> my_frame.drop_columns(["column_b", "column_d"])
    >>> print my_frame.schema
    [("column_a", str), ("column_c", str)]


Now the frame only has the columns *column_a* and *column_c*.
For further examples, see: ref:`example_frame.drop_columns`.


