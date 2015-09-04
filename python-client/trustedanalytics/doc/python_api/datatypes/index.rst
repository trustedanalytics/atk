.. index::
    pair: data; type

.. _api_datatypes:

Data Types
==========

All data manipulated and stored using frames and graphs must fit into one of
the supported Python data types.

.. code::

    >>> ta.valid_data_types

    float32, float64, ignore, int32, int64, unicode, vector(n), datetime
    (and aliases: float->float64, int->int32, list->vector, long->int64, str->unicode)


==============  =========================================================================================
**datetime**    |ALPHA| object for date and time; equivalent to python's datetime.datetime class.
                Converts to and from strings using the ISO 8601 format.  Inside the server, the object
                is represented by the nscala/joda DateTime object.  When interfacing with various data
                sources and sinks that use different data types for datetime, the datetime value will
                be converted to a string by default.

**float32**     32-bit floating point number; equivalent to numpy.float32

**float64**     64-bit floating point number; equivalent to numpy.float64

**ignore**      type available to describe a field in a data source that the
                parser should ignore

**int32**       32-bit integer; equivalent to numpy.int32

**int64**       32-bit integer; equivalent to numpy.int64

**unicode**     Python's unicode representation for strings.

**vector(n)**   |ALPHA|  Ordered list of n float64 numbers (array of fixed-length n); uses numpy.ndarray
==============  =========================================================================================


**Note:**  Numpy values of positive infinity (np.inf), negative infinity
(-np.inf) or nan (np.nan) are treated as Python's None when sent to the server.
Results of any user-defined functions which deal with such values are
automatically converted to None.
Any further usage of those data points should treat the values as None.


.. only:: latex

    API Maturity Tags

        Functions in the API may be at different levels of software maturity.
        Where a function is not mature, the documentation will note it with one
        of the following tags.  The absence of a tag means the function is
        standardized and fully tested.

        |ALPHA|

        |BETA|

        |DEPRECATED|
