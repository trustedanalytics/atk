Examples
--------
Consider the following sample data set\:

.. code::

    >>> frame.inspect()

      a:unicode  b:int32
    /--------------------/
        a          2
        b          7
        c          3
        d          9
        e          1

A simple call for 3 equal-width bins gives\:

.. code::

    >>> hist = frame.histogram("b", num_bins=3)
    >>> print hist

    Histogram:
        cutoffs: cutoffs: [1.0, 3.6666666666666665, 6.333333333333333, 9.0],
        hist: [3, 0, 2],
        density: [0.6, 0.0, 0.4]


Switching to equal depth gives\:

.. code::

    >>> hist = frame.histogram("b", num_bins=3, bin_type='equaldepth')
    >>> print hist

    Histogram:
        cutoffs: [1, 2, 7, 9],
        hist: [1, 2, 2],
        density: [0.2, 0.4, 0.4]


.. only:: html

       Plot hist as a bar chart using matplotlib\:

    .. code::

        >>> import matplotlib.pyplot as plt

        >>> plt.bar(hist.cutoffs[:1], hist.hist, width=hist.cutoffs[1] - hist.cutoffs[0])

.. only:: latex

       Plot hist as a bar chart using matplotlib\:

    .. code::

        >>> import matplotlib.pyplot as plt

        >>> plt.bar(hist.cutoffs[:1], hist.hist, width=hist.cutoffs[1] - 
        ... hist.cutoffs[0])

