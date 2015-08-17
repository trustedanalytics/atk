Examples
--------
For this example, my_frame is a Frame object with lots of data for the
attributes of ``lizards``, ``frogs``, and ``snakes``.
Get rid of everything, except information about lizards and frogs:

code::

    >>> def my_filter(row):
    ... return row['animal_type'] == 'lizard' or
    ... row['animal_type'] == "frog"

    >>> my_frame.filter(my_filter)

The frame now only has data about ``lizards`` and ``frogs``.

More information on a UDF can be found at `Python User Functions`_
