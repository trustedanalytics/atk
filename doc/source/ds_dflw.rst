.. _ds_dflw:

.. index::
    single: example

=====================
Process Flow Examples
=====================

The information in this section displays the general path taken to build
using |ATK| with Python.

.. contents:: Table of Contents
    :local:
    :backlinks: none

.. index::
    single: Python

-----------------
Python Path Setup
-----------------

.. _pythonpath:

The location of the 'trustedanalytics' directory should be added
to the PYTHONPATH environmental variable prior to starting Python.
This can be done from a shell script, like this::

    export PYTHONPATH=$PYTHONPATH:/usr/lib/ # appends path where trustedanalytics directory exists to
                                            # any existing path
                                            # this could also be added to .bashrc or other profile script
    python  # starts python CLI

This way, from inside Python, it is easier to load and connect to the
REST server:

.. code::

    >>> import trustedanalytics as ta
    >>> ta.connect()

.. index::
    pair: data; type

.. _valid_data_types:

--------
Raw Data
--------

Data is made up of variables of heterogeneous types (for example: strings,
integers, and floats) that can be organized as a collection of rows and
columns, similar to a table or spreadsheet.
Each row corresponds to the data associated with one observation, and each
column corresponds to a variable being observed.
See the Python API :ref:`Data Types <python_api/datatypes/index>` for
a current list of data types supported.

To see the data types supported:

.. code::

    >>> print ta.valid_data_types

You should see a list of variable types similar to this:

.. code::

    float32, float64, int32, int64, unicode
    (and aliases: float->float64, int->int32, long->int64, str->unicode)

.. note::

    Although |PACKAGE| utilizes the NumPy package, NumPy values of positive
    infinity (np.inf), negative infinity (-np.inf) or nan (np.nan) are treated
    as None.
    Results of any user-defined functions which deal with such values are
    automatically converted to None, so any further usage of those data points
    should treat the values as None.

.. _Importing Data:

.. index::
    single: import

Ingesting the Raw Data
======================

See the API section :ref:`Data Sources <python_api/datasources/index>`
for the various methods of ingesting data.


.. _example_files.CsvFile:

Importing a |CSV| file.
-----------------------

These are some rows from a |CSV| file::

    "string",123,"again",25.125
    "next",5,"or not",1.0
    "fail",1,"again?",11.11

|CSV| files contain rows of information separated by new-line characters.
Within each row, the data fields are separated from each other by some standard
character.
In the above example, the separating character is a comma (,).

To import data, you must tell the system how the input file is formatted.
This is done by defining a :term:`schema`.
Schemas are constructed as a list of tuples, each of which contains an
:term:`ASCII`-character name and the item's :ref:`data type <valid_data_types>`,
ordered according to the order of columns in the input file.

Given a file *datasets/small_songs.csv* whose contents look like this::

    1,"Easy on My Mind"
    2,"No Rest For The Wicked"
    3,"Does Your Chewing Gum"
    4,"Gypsies, Tramps, and Thieves"
    5,"Symphony No. 5"

Create a variable to hold the file name (for easier reuse):

.. code::

    >>> my_data_file = "datasets/small_songs.csv"

Create the schema *my_schema* with two columns: *id* (int32), and *title*
(str):

.. code::

    >>> my_schema = [('id', ta.int32), ('title', str)]

The schema and file name are used in the CsvFile() command to describe the file
format:

.. code::

    >>> my_csv_description = ta.CsvFile(my_data_file, my_schema)

The data fields are separated by a character delimiter.
The default delimiter to separate column data is a comma.
It can be changed with the parameter *delimiter*:

.. code::

    >>> my_csv_description = ta.CsvFile(my_data_file, my_schema, delimiter = ",")

This can be helpful if the delimiter is something other than a comma, for
example, ``\t`` for tab-delimited records.

Occasionally, there are header lines in the data file.
For example, these lines may describe the source or format of the data.
If there are lines at the beginning of the file, they should be skipped by
the import mechanism.
The number of lines to skip is specified by the *skip_header_lines*
parameter:

.. code::

    >>> csv_description = ta.CsvFile(my_data_file, my_schema, skip_header_lines = 5)

.. warning::

    See :ref:`Errata` for an issue skipping header lines.

Now we use the schema and the file name to create CsvFile objects, which define
the data layouts:

.. only:: html

    .. code::

        >>> my_csv = ta.CsvFile(my_data_file, my_schema)
        >>> csv1 = ta.CsvFile(file_name="data1.csv", schema=schema_ab)
        >>> csv2 = ta.CsvFile(file_name="more_data.txt", schema=schema_ab)

        >>> raw_csv_data_file = "datasets/my_data.csv"
        >>> column_schema_list = [("x", ta.float64), ("y", ta.float64), ("z", str)]
        >>> csv4 = ta.CsvFile(file_name=raw_csv_data_file, schema=column_schema_list, delimiter='|', skip_header_lines=2)


.. only:: latex

    .. code::

        >>> my_csv = ta.CsvFile(my_data_file, my_schema)
        >>> csv1 = ta.CsvFile(file_name="data1.csv", schema=schema_ab)
        >>> csv2 = ta.CsvFile(file_name="more_data.txt", schema=schema_ab)

        >>> raw_csv_data_file = "datasets/my_data.csv"
        >>> column_schema_list = [("x", ta.float64), ("y", ta.float64), ("z", str)]
        >>> csv4 = ta.CsvFile(file_name=raw_csv_data_file,
        ... schema=column_schema_list, delimiter='|', skip_header_lines=2)


.. _example_frame.frame:

------
Frames
------

A Frame is a class of objects capable of accessing and controlling "big data".
The data can be visualized as a two-dimensional table structure of rows and
columns.
|PACKAGE| can handle frames with large volumes of data, because it is
designed to work with data spread over multiple machines.

Create a Frame
==============

There are several ways to create frames\:

#.  As "empty", with no schema or data
#.  With a schema and data
#.  By copying (all or a part of) another frame
#.  As a return value from a Frame-based method; this is part of the
    :term:`ETL <Extract, Transform, and Load>` data flow.

See the Python API :ref:`Frames section <python_api/frames/index>` for more information.

Examples:
---------
Create an empty frame:

.. code::

    >>> my_frame = ta.Frame()

The Frame *my_frame* is now a Python object which references an empty frame
that has been created on the server.

To create a frame defined by the schema *my_csv* (see
:ref:`example_files.CsvFile`), import the data, give the frame the name
*myframe*, and create a Frame object, *my_frame*, to access it:

.. code::

    >>> my_frame = ta.Frame(source=my_csv, name='myframe')

To copy the frame *myframe*, create a Frame *my_frame2* to access it, and give
it a new name, because the name must always be unique:

.. code::

    >>> my_frame2 = my_frame.copy(name = "copy_of_myframe")

To create a new frame with only columns *x* and *z* from the original frame
*myframe*, and save the Frame object as *my_frame3*:

.. code::

    >>> my_frame3 = my_frame.copy(['x', 'z'], name = "copy2_of_myframe")

To create a frame copy of the original columns *x* and *z*, but only those
rows where *z* is TRUE:

.. code::

    >>> my_frame4 = my_frame.copy(['x', 'z'], where = (lambda row: Harry Potter in row.title),
    ... name = "copy_of_myframe_true")

Frames are handled by reference.
Commands such as :code:`f4 = my_frame` will only give you a copy of the Frame
proxy pointing to the same data.

Let's create a Frame and examine its contents:

.. code::

    >>> small_songs = ta.Frame(my_csv, name = "small_songs")
    >>> small_songs.inspect()
    >>> small_songs.get_error_frame().inspect()

.. index::
    pair: append; example
    single: append

.. _example_frame.append:

Append:
-------
The :ref:`append <python_api/frames/frame-/append>` method adds rows and columns of data to a frame.
Columns and rows are added to the database structure, and data is imported
as appropriate.
If columns are the same in both name and data type, the appended data will
go into the existing column.

As an example, let's start with a frame containing two columns *a* and *b*.
The frame can be accessed by Frame *my_frame1*.
We can look at the data and structure of the database by using the :ref:`inspect
<python_api/frames/frame-/inspect>` method:

.. code::

    >>> my_frame1.inspect()

      a:str   b:ta.int64
    /--------------------/
      apple          182
      bear            71
      car           2048

Given another frame, accessed by Frame *my_frame2* with one column *c*:

.. code::

    >>> my_frame2.inspect()

      c:str
    /-------/
      dog
      cat

With :ref:`append <python_api/frames/frame-/append>`:

.. code::

    >>> my_frame1.append(my_frame2)

The result is that the first frame would have the data from both frames.
It would still be accessed by Frame *my_frame1*:

.. code::

    >>> my_frame1.inspect()

      a:str     b:ta.int64     c:str
    /--------------------------------/
      apple        182         None
      bear          71         None
      car         2048         None
      None        None         dog
      None        None         cat

.. only:: html

    Try this example with data files *objects1.csv* and *objects2.csv*::

        >>> objects1 = ta.Frame(ta.CsvFile("datasets/objects1.csv", schema=[('Object', str), ('Count', ta.int64)], skip_header_lines=1), 'objects1')
        >>> objects2 = ta.Frame(ta.CsvFile("datasets/objects2.csv", schema=[('Thing', str)], skip_header_lines=1), 'objects2')

        >>> objects1.inspect()
        >>> objects2.inspect()

        >>> objects1.append(objects2)
        >>> objects1.inspect()

.. only:: latex

    Try this example with data files *objects1.csv* and *objects2.csv*::

        >>> objects1 = ta.Frame(ta.CsvFile("datasets/objects1.csv",
        ... schema=[('Object', str), ('Count', ta.int64)],
        ... skip_header_lines=1), 'objects1')
        >>> objects2 = ta.Frame(ta.CsvFile("datasets/objects2.csv",
        ... schema=[('Thing', str)], skip_header_lines=1), 'objects2')

        >>> objects1.inspect()
        >>> objects2.inspect()

        >>> objects1.append(objects2)
        >>> objects1.inspect()

See also the :ref:`join <python_api/frames/frame-/join>` method in the
:doc:`API <python_api/index>` section.

.. _example_frame.inspect:

Inspect the Data
================
|PACKAGE| provides several methods that allow you to inspect your data,
including :ref:`inspect <python_api/frames/frame-/inspect>` and
:ref:`take <python_api/frames/frame-/take>`.
The Frame class also contains frame information like
:ref:`row_count <python_api/frames/frame-/row_count>`.

Examples
--------
To see the number of rows:

.. code::

    >>> objects1.row_count

To see the number of columns:

.. code::

    >>> len(objects1.schema)

To see the Frame pointer:

.. code::

    >>> objects1

To see two rows of data:

.. code::

    >>> objects1.inspect(2)

Gives you something like this:

.. code::

      a:ta.float64  b:ta.int64
    /--------------------------/
        12.3000            500
       195.1230         183954

The take() method makes a list of lists of frame data.
Each list has the data from a row in the frame accessed by the Frame,
in this case, 3 rows beginning from row 2.

.. code::

    >>> subset_of_objects1 = objects1.take(3, offset=2)
    >>> print subset_of_objects1

Gives you something like this:

.. code::

    [[12.3, 500], [195.123, 183954], [12.3, 500]]

.. note::

    The row sequence of the data is NOT guaranteed to match the sequence of the
    input file.
    When the data is spread out over multiple clusters, the original sequence
    of rows from the raw data is lost.
    Also, the sequence order of the columns is changed (from original data)
    by some commands.

.. only:: html

    Some more examples to try:

    .. code::

        >>> animals = ta.Frame(ta.CsvFile("datasets/animals.csv", schema=[('User', ta.int32), ('animals', str), ('int1', ta.int64), ('int2', ta.int64), ('Float1', ta.float64), ('Float2', ta.float64)], skip_header_lines=1), 'animals')
        >>> animals.inspect()
        >>> freq = animals.top_k('animals', animals.row_count)
        >>> freq.inspect(freq.row_count)

        >>> from pprint import *
        >>> summary = {}
        >>> for col in ['int1', 'int2', 'Float1', 'Float2']:
        ...     summary[col] = animals.column_summary_statistics(col)
        ...     pprint(summary[col])

.. only:: latex

    Some more examples to try:

    .. code::

        >>> animals = ta.Frame(ta.CsvFile("datasets/animals.csv",
        ... schema=[('User', ta.int32), ('animals', str), ('int1', ta.int64),
        ... ('int2', ta.int64), ('Float1', ta.float64), ('Float2',
        ... ta.float64)], skip_header_lines=1), 'animals')
        >>> animals.inspect()
        >>> freq = animals.top_k('animals', animals.row_count)
        >>> freq.inspect(freq.row_count)

        >>> from pprint import *
        >>> summary = {}
        >>> for col in ['int1', 'int2', 'Float1', 'Float2']:
        ...     summary[col] = animals.column_summary_statistics(col)
        ...     pprint(summary[col])


Clean the Data
==============

The process of "data cleaning" encompasses the identification and removal or
repair of incomplete, incorrect, or malformed information in a data set.
The |PACKAGE| Python API provides much of the functionality necessary for these
tasks.
It is important to keep in mind that it was designed for data scalability.
Thus, using external Python packages for these tasks, while possible, may
not provide the same level of efficiency.

.. warning::

    Unless stated otherwise, cleaning functions use the Frame proxy to
    operate directly on the data, so they change the data in the frame,
    rather than return a new frame with the changed data.
    Copying the data to a new frame on a regular
    basis and work on the new frame,
    provides a fallback if something does not work as expected:

    .. code::

        >>> next_frame = current_frame.copy()

In general, the following functions select rows of data based upon the data
in the row.
For details about row selection based upon its data see :doc:`/ds_apir`.

Example of data cleaning:

.. code::

    >>> def clean_animals(row):
    ...     if 'basset hound' in row.animals:
    ...         return 'dog'
    ...     elif 'guinea pig' in row.animals:
    ...         return 'cavy'
    ...     else:
    ...         return row.animals

    >>> animals.add_columns(clean_animals, ('animals_cleaned', str))
    >>> animals.drop_columns('animals')
    >>> animals.rename_columns({'animals_cleaned' : 'animals'})

.. index::
    pair: drop rows; example
    single: drop rows

.. _example_frame.drop_rows:

Drop Rows:
----------

The :ref:`drop_rows <python_api/frames/frame-/drop_rows>`
method takes a predicate function and removes all rows for
which the predicate evaluates to ``True``.

Examples:
~~~~~~~~~
Drop any rows in the animals frame where the value in column *int2* is
negative:

.. code::

    >>> animals.drop_rows(lambda row: row['int2'] < 0)

To drop any rows where *a* is empty:

.. code::

    >>> my_frame.drop_rows(lambda row: row['a'] is None)

To drop any rows where any column is empty:

.. code::

    >>> my_frame.drop_rows(lambda row: any([cell is None for cell in row]))

.. index::
    pair: filter rows; example
    single: filter rows

.. _example_frame.filter:

Filter Rows:
------------

The :ref:`filter <python_api/frames/frame-/filter>` method is like
:ref:`drop_rows <python_api/frames/frame-/drop_rows>`, except it removes all rows
for which the predicate evaluates to False.

Examples:
~~~~~~~~~

To delete those rows where field *b* is outside the range of 0 to 10:

.. code::

    >>> my_frame.filter(lambda row: 0 >= row['b'] AND row['b'] >= 10)

.. index::
    pair: drop duplicates; example
    single: drop duplicates
    single: duplicates

.. _example_frame.drop_duplicates:

Drop Duplicates:
----------------

The :ref:`drop_duplicates <python_api/frames/frame-/drop_duplicates>`
method performs a row uniqueness comparison across the whole table.

Examples:
~~~~~~~~~

To drop any rows where the data in *a* and column *b* are
duplicates of some previously evaluated row:

.. code::

    >>> my_frame.drop_duplicates(['a', 'b'])

To drop all duplicate rows where the columns *User* and *animals*
have the same values as in some previous row:

.. code::

    >>> animals.drop_duplicates(['User', 'animals'])

.. index::
    pair: drop column; example
    single: drop column

.. _example_frame.drop_columns:

Drop Columns:
-------------

Columns can be dropped either with a string matching the column name or a
list of strings:

.. code::

    >>> my_frame.drop_columns('b')
    >>> my_frame.drop_columns(['a', 'c'])

.. index::
    pair: rename column; example
    single: rename column

.. _example_frame.rename_columns:

Rename Columns:
---------------

Columns can be renamed by giving the existing column name and the new name,
in the form of a dictionary.
Unicode characters should not be used for column names.

Rename *a* to "id":

.. code::

    >>> my_frame.rename_columns({'a': 'id'})

Rename column *b* to "author" and *c* to "publisher":

.. code::

    >>> my_frame.rename_columns({'b': 'author', 'c': 'publisher'})

Transform the Data
==================

Often, you will need to create new data based upon the existing data.
For example, you need the first name combined with the last name, or
you need the number of times John spent more than five dollars, or
you need the average age of students attending a college.

.. index::
    pair: add column; example
    single: add column

.. _example_frame.add_columns:

Add Columns:
------------

Columns can be added to the frame using values from other columns as their
value.

.. only:: html

    Add a column *int1_times_int2* as an ta.float64 and fill it with the contents
    of column *int1* and column *int2* multiplied together:

    .. code::

        >>> animals.add_columns(lambda row: row.int1*row.int2, ('int1xint2', ta.float64))

.. only:: latex

    Add a column *int1_times_int2* as an ta.float64 and fill it with the contents
    of column *int1* and column *int2* multiplied together:

    .. code::

        >>> animals.add_columns(lambda row: row.int1*row.int2, ('int1xint2',
        ... ta.float64))

Add a new column *all_ones* and fill the entire column with the value 1:

.. code::

    >>> animals.add_columns(lambda row: 1, ('all_ones', ta.int64))

.. only:: html

    Add a new column *float1_plus_float2* and fill the entire column with the
    value of column *float1* plus column *float2*, then save a summary of
    the frame statistics:

    .. code::

        >>> animals.add_columns(lambda row: row.Float1 + row.Float2, ('Float1PlusFloat2', ta.float64))
        >>> summary = animals.column_summary_statistics('Float1PlusFloat2')

.. only:: latex

    Add a new column *float1_plus_float2* and fill the entire column with the
    value of column *float1* plus column *float2*, then save a summary of
    the frame statistics::

        >>> animals.add_columns(lambda row: row.Float1 + row.Float2,
        ... ('Float1PlusFloat2', ta.float64))
        >>> summary = animals.column_summary_statistics('Float1PlusFloat2')

Add a new column *pwl*, type ta.float64, and fill the value according to
this table:

+---------------------------------------+------------------------------------+
| value in column *float1_plus_float2*  | value for column *pwl*             |
+=======================================+====================================+
| None                                  | None                               |
+---------------------------------------+------------------------------------+
| Less than 50                          | *float1_plus_float2*               |
|                                       | times 0.0046 plus 0.4168           |
+---------------------------------------+------------------------------------+
| At least 50 and less than 81          | *float1_plus_float2*               |
|                                       | times 0.0071 plus 0.3429           |
+---------------------------------------+------------------------------------+
| At least 81                           | *float1_plus_float2*               |
|                                       | times 0.0032 plus 0.4025           |
+---------------------------------------+------------------------------------+
| None of the above                     | None                               |
+---------------------------------------+------------------------------------+

An example of Piecewise Linear Transformation:

.. code::

    >>> def piecewise_linear_transformation(row):
    ...    x = row.float1_plus_float2
    ...    if x is None:
    ...        return None
    ...    elif x < 50:
    ...        m, c =0.0046, 0.4168
    ...    elif 50 <= x AND x < 81:
    ...        m, c =0.0071, 0.3429
    ...    elif 81 <= x:
    ...        m, c =0.0032, 0.4025
    ...    else:
    ...        return None
    ...    return m * x + c

    >>> animals.add_columns(piecewise_linear_transformation, ('pwl', ta.float64))

.. only:: html

    Create multiple columns at once by making a function return a list of
    values for the new frame columns:

    .. code::

        >>> animals.add_columns(lambda row: [abs(row.int1), abs(row.int2)], [('abs_int1', ta.int64), ('abs_int2', ta.int64)])

.. only:: latex

    Create multiple columns at once by making a function return a list of
    values for the new frame columns:

    .. code::

        >>> animals.add_columns(lambda row: [abs(row.int1), abs(row.int2)],
        ... [('abs_int1', ta.int64), ('abs_int2', ta.int64)])

.. _ds_dflw_frame_examine:

.. index::
    single: statistics

Examining the Data
==================

To get standard descriptive statistics information about my_frame, use the
frame function :ref:`column_summary_statistics
<python_api/frames/frame-/column_summary_statistics>`:

.. code::

    >>> my_frame.column_summary_statistics()

.. index::
    pair: group by; example
    single: group by

.. _example_frame.group_by:

Group by (and aggregate):
-------------------------

Rows can be grouped together based on matching column values, after which an
aggregation function can be applied on each group, producing a new frame.

Example process of using aggregation based on columns:

#.  given our frame of animals
#.  create a new frame and a Frame *grouped_animals* to access it
#.  group by unique values in column *animals*
#.  average the grouped values in column *int1* and save it in a
    column *int1_avg*
#.  add up the grouped values in column *int1* and save it in a
    column *int1_sum*
#.  get the standard deviation of the grouped values in column
    *int1* and save it in a column *int1_stdev*
#.  average the grouped values in column *int2* and save it in a
    column *int2_avg*
#.  add up the grouped values in column *int2* and save it in a
    column *int2_sum*

.. only:: html

    .. code::

        >>> grouped_animals = animals.group_by('animals', {'int1': [ta.agg.avg, ta.agg.sum, ta.agg.stdev], 'int2': [ta.agg.avg, ta.agg.sum]})
        >>> grouped_animals.inspect()

.. only:: latex

    .. code::

        >>> grouped_animals = animals.group_by('animals', {'int1': [ta.agg.avg,
        ... ta.agg.sum, ta.agg.stdev], 'int2': [ta.agg.avg, ta.agg.sum]})
        >>> grouped_animals.inspect()

.. note::

    The only columns in the new frame will be the grouping column and the
    generated columns.
    In this case, regardless of the original frame size, you will get six
    columns.

Example process of using aggregation based on both column and row together:

#.  Using our data accessed by *animals*, create a new frame and a Frame
    *grouped_animals2* to access it
#.  Group by unique values in columns *animals* and *int1*
#.  Using the data in the *float1* column, calculate each group's average,
    standard deviation, variance, minimum, and maximum
#.  Count the number of rows in each group and put that value in column
    *int2_count*
#.  Count the number of distinct values in column *int2* for each group and
    put that number in column *int2_count_distinct*

.. only:: html

    .. code::

        >>> grouped_animals2 = animals.group_by(['animals', 'int1'], {'Float1': [ta.agg.avg, ta.agg.stdev, ta.agg.var, ta.agg.min, ta.agg.max], 'int2': [ta.agg.count, ta.agg.count_distinct]})

.. only:: latex

    .. code::

        >>> grouped_animals2 = animals.group_by(['animals', 'int1'], {'Float1':
        ... [ta.agg.avg, ta.agg.stdev, ta.agg.var, ta.agg.min, ta.agg.max],
        ... 'int2': [ta.agg.count, ta.agg.count_distinct]})

Example process of using aggregation based on row:

#.  Using our data accessed by *animals*, create a new frame and a Frame
    *grouped_animals2* to access it
#.  Group by unique values in columns *animals* and *int1*
#.  Count the number of rows in each group and put that value in column
    *count*

.. only:: html

    .. code::

        >>> grouped_animals2 = animals.group_by(['animals', 'int1'], ta.agg.count)

.. only:: latex

    .. code::

        >>> grouped_animals2 = animals.group_by(['animals', 'int1'],
        ... ta.agg.count)

.. _aggregation_functions:

.. note::

    :code:`agg.count` is the only full row aggregation function supported at
    this time.

Aggregation currently supports using the following functions:

..  hlist::
    :columns: 5

    * avg
    * count
    * count_distinct
    * max
    * min
    * stdev
    * sum
    * var (see glossary :term:`Bias vs Variance`)

.. index::
    pair: join; example
    single: join

.. _example_frame.join:

Join:
-----

Create a **new** frame from a :ref:`join <python_api/frames/frame-/join>`
operation with another frame.

Given two frames *my_frame* (columns *a*, *b*, *c*) and *your_frame* (columns
*b*, *c*, *d*).
For the sake of readability, in these examples we will refer to the frames and
the Frames by the same name, unless needed for clarity:

.. code::

    >>> my_frame.inspect()

      a:str       b:str       c:str
    /-----------------------------------/
      alligator   bear        cat
      auto        bus         car
      apple       berry       cantaloupe
      mirror      frog        ball

    >>> your_frame.inspect()

      b:str       c:ta.int64     d:str
    /----------------------------------/
      bus             871        dog
      berry          5218        frog
      blue              0        log

Column *b* in both frames is a unique identifier used to relate the two frames.
Following this instruction will join *your_frame* to *my_frame*, creating a new
frame with a new Frame to access it, with all of the data from *my_frame* and
only that data from *your_frame* which has a value in *b* that matches a value
in *my_frame* *b*:

.. code::

    >>> our_frame = my_frame.join(your_frame, 'b', how='left')

The result is *our_frame*:

.. code::

    >>> our_frame.inspect()

      a:str       b:str       c_L:str      c_R:ta.int64   d:str
    /-----------------------------------------------------------/
      alligator   bear        cat          None           None
      auto        bus         car           871           dog
      apple       berry       cantaloupe   5281           frog
      mirror      frog        ball         None           None

Doing an "inner" join this time will include only data from *my_frame* and
*your_frame* which have matching values in *b*:

.. code::

    >>> inner_frame = my_frame.join(your_frame, 'b')

or

.. code::

    >>> inner_frame = my_frame.join(your_frame, 'b', how='inner')

Result is *inner_frame*:

.. code::

    >>> inner_frame.inspect()

      a:str       b:str       c_L:str      c_R:ta.int64   d:str
    /-----------------------------------------------------------/
      auto        bus         car             871         dog
      apple       berry       cantaloupe     5218         frog

Doing an "outer" join this time will include only data from *my_frame* and
*your_frame* which do not have matching values in *b*:

.. code::

    >>> outer_frame = my_frame.join(your_frame, 'b', how='outer')

Result is *outer_frame*:

.. code::

    >>> outer_frame.inspect()

      a:str       b:str       c_L:str      c_R:ta.int64   d:str
    /-----------------------------------------------------------/
      alligator   bear        cat            None         None
      mirror      frog        ball           None         None
      None        blue        None              0         log

If column *b* in *my_frame* and column *d* in *your_frame* are the common
column:
Doing it again but including all data from *your_frame* and only that data
in *my_frame* which has a value in *b* that matches a value in
*your_frame* *d*:

.. only:: html

    .. code::

        >>> right_frame = my_frame.join(your_frame, left_on='b', right_on='d', how='right')

.. only:: latex

    .. code::

        >>> right_frame = my_frame.join(your_frame, left_on='b', right_on='d',
        ... how='right')

Result is *right_frame*:

.. code::

    >>> right_frame.inspect()

      a:str      b_L:str      c:str      b_R:str    c:ta.int64   d:str
    /---------------------------------------------------------------------/
      None       None         None       bus         871         dog
      mirror     frog         ball       berry      5218         frog
      None       None         None       blue          0         log

.. index::
    pair: flatten column; example
    single: flatten column

.. _example_frame.flatten_column:

Flatten Column:
---------------

The function :ref:`flatten_column <python_api/frames/frame-/flatten_column>`
creates a **new** frame by splitting a
particular column and returns a Frame object.
The column is searched for rows where there is more than one value,
separated by commas.
The row is duplicated and that column is spread across the existing and new
rows.

Given a frame accessed by Frame *my_frame* and the frame has two columns
*a* and *b*.
The "original_data"::

    1-"solo,mono,single"
    2-"duo,double"

Bring the data in where it can by worked on:

.. only:: html

    .. code::

        >>> my_csv = ta.CsvFile("original_data.csv", schema=[('a', ta.int64), ('b', str)], delimiter='-')
        >>> my_frame = ta.Frame(source=my_csv)

.. only:: latex

    .. code::

        >>> my_csv = ta.CsvFile("original_data.csv", schema=[('a', ta.int64),
        ... ('b', str)], delimiter='-')
        >>> my_frame = ta.Frame(source=my_csv)

Check the data:

.. code::

    >>> my_frame.inspect()

      a:ta.int64   b:string
    /---------------------------------/
          1        solo, mono, single
          2        duo, double

Spread out those sub-strings in column *b*:

.. code::

    >>> your_frame = my_frame.flatten_column('b')

Now check again and the result is:

.. code::

    >>> your_frame.inspect()

      a:ta.int64   b:str
    /-----------------------/
        1          solo
        1          mono
        1          single
        2          duo
        2          double

.. _ds_dflw_building_a_graph:

--------------
Seamless Graph
--------------

For the examples below, we will use a Frame *my_frame*, which accesses an
arbitrary frame of data consisting of the :download:`following
<_downloads/employees.csv>`:

+----------+---------+-------------------+-------+
| Employee | Manager | Title             | Years |
+==========+=========+===================+=======+
| Bob      | Steve   | Associate         | 1     |
+----------+---------+-------------------+-------+
| Jane     | Steve   | Sn Associate      | 3     |
+----------+---------+-------------------+-------+
| Anup     | Steve   | Associate         | 3     |
+----------+---------+-------------------+-------+
| Sue      | Steve   | Market Analyst    | 1     |
+----------+---------+-------------------+-------+
| Mohit    | Steve   | Associate         | 2     |
+----------+---------+-------------------+-------+
| Steve    | David   | Marketing Manager | 5     |
+----------+---------+-------------------+-------+
| Larry    | David   | Product Manager   | 3     |
+----------+---------+-------------------+-------+
| David    | Rob     | VP of Sales       | 7     |
+----------+---------+-------------------+-------+

Build the Graph
===============

Make an empty graph and give it a name:

.. code::

    >>> my_graph = ta.graph()
    >>> my_graph.name = "personnel"

Define the vertex types:

.. code::

    >>> my_graph.define_vertex_type("employee")
    >>> my_graph.define_vertex_type("manager")
    >>> my_graph.define_vertex_type("title")
    >>> my_graph.define_vertex_type("years")

Define the edge type:

.. code::

    >>> my_graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=True)

Add the data:

.. only:: html

    .. code::

        >>> my_graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
        >>> my_graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'], create_missing_vertices = True)

.. only:: latex

    .. code::

        >>> my_graph.vertices['Employee'].add_vertices(employees_frame,
        ... 'Employee', ['Title'])
        >>> my_graph.edges['worksunder'].add_edges(employees_frame, 'Employee',
        ... 'Manager', ['Years'], create_missing_vertices = True)

.. warning::

    Improperly built graphs can give inconsistent results.
    For example, given EdgeFrames with this data::

        Movieid, movieTitle, Rating, userId
        1, Titanic, 3, 1
        1, My Own Private Idaho, 3, 2

    If the vertices are built out of this data, the vertex with Movieid of 1
    would sometimes have the Titanic data and sometimes would have the Idaho
    data, based upon which order the records are delivered to the function.


Other Graph Options
===================

Inspect the graph:

.. code::

    >>> my_graph.vertex_count
    >>> my_graph.edge_count
    >>> my_graph.vertices['Employee'].inspect(20)
    >>> my_graph.edges['worksunder'].inspect(20)

For further information, see the API section on :ref:`Graphs <python_api/graphs/index>`.

.. _export_to_titan:

Export the graph to a TitanGraph:

.. code::

    >>> my_titan_graph = my_graph.export_to_titan("titan_graph")

Make a VertexFrame:

.. code::

    >>> my_vertex_frame = my_graph.vertices("employee")

Make an EdgeFrame:

.. code::

    >>> my_edge_frame = my_graph.edges("worksunder")

-----------
Titan Graph
-----------

Graph Creation
==============

A Titan graph is created by exporting it from a seamless graph.
For further information, as well as Titan graph attributes and methods, see the
API section on :ref:`Titan Graph <python_api/graphs/graph-titan/index>`.

.. _Graph_Analytics:

.. index::
    pair: analytics; graph

.. toctree::
    :hidden:

    ds_apir
    CollaborativeFilteringNewPlugin_Summary
    LdaNewPlugin_Summary

------
Models
------

Collaborative Filtering
-----------------------
See the :ref:`models section of the API
<python_api/models/model-collaborative_filtering/index>` for details.

Graphical Models
----------------
Graphical models find more insights from structured noisy data.
See :ref:`graph API <python_api/graphs/index>` for details of the
:term:`Label Propagation` (LP) and :term:`Loopy Belief Propagation` (LBP).

Topic Modeling
--------------
For Topic Modeling, see the :ref:`LDA Model section of the API
<python_api/models/model-lda/index>` and
http://en.wikipedia.org/wiki/Topic_model
