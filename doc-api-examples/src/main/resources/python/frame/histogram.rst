Examples
--------
<hide>
    >>> import trustedanalytics as ta
    >>> ta.connect()
    <connect>

    >>> data = [['a', 2], ['b', 7], ['c', 3], ['d', 9], ['e', 1]]
    >>> schema = [('a', unicode), ('b', ta.int32)]

    >>> frame = ta.Frame(ta.UploadRows(data, schema))
    <progress>

</hide>

Consider the following sample data set\:

.. code::

    >>> frame.inspect()
        [#]  a  b
        =========
        [0]  a  2
        [1]  b  7
        [2]  c  3
        [3]  d  9
        [4]  e  1

A simple call for 3 equal-width bins gives\:

.. code::

    >>> hist = frame.histogram("b", num_bins=3)
    <progress>

    >>> print hist
    Histogram:
    cutoffs: [1.0, 3.6666666666666665, 6.333333333333333, 9.0],
    hist: [3.0, 0.0, 2.0],
    density: [0.6, 0.0, 0.4]

Switching to equal depth gives\:

.. code::

    >>> hist = frame.histogram("b", num_bins=3, bin_type='equaldepth')
    <progress>

    >>> print hist
    Histogram:
    cutoffs: [1.0, 2.0, 7.0, 9.0],
    hist: [1.0, 2.0, 2.0],
    density: [0.2, 0.4, 0.4]

.. only:: html

       Plot hist as a bar chart using matplotlib\:

    .. code::
<skip>
        >>> import matplotlib.pyplot as plt

        >>> plt.bar(hist.cutoffs[:1], hist.hist, width=hist.cutoffs[1] - hist.cutoffs[0])
</skip>
.. only:: latex

       Plot hist as a bar chart using matplotlib\:

    .. code::
<skip>
        >>> import matplotlib.pyplot as plt

        >>> plt.bar(hist.cutoffs[:1], hist.hist, width=hist.cutoffs[1] - 
        ... hist.cutoffs[0])
</skip>
