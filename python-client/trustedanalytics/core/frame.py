#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Frame entity types
"""

import logging
logger = logging.getLogger(__name__)

from trustedanalytics.meta.context import api_context
from trustedanalytics.core.decorators import *
api = get_api_decorator(logger)

from trustedanalytics.core.api import api_status
from trustedanalytics.core.column import Column
from trustedanalytics.core.errorhandle import IaError
from trustedanalytics.meta.udf import has_udf_arg
from trustedanalytics.meta.namedobj import name_support
from trustedanalytics.meta.metaprog import CommandInstallable as CommandLoadable
from trustedanalytics.meta.docstub import doc_stubs_import
from trustedanalytics.core.ui import inspect_settings


def _get_backend():
    from trustedanalytics.meta.config import get_frame_backend
    return get_frame_backend()


# BaseFrame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from trustedanalytics.core.docstubs1 import _DocStubs_BaseFrame
    doc_stubs_import.success(logger, "_DocStubsBaseFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubsBaseFrame", e)
    class _DocStubs_BaseFrame(object): pass


# Frame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from trustedanalytics.core.docstubs1 import _DocStubsFrame
    doc_stubs_import.success(logger, "_DocStubsFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubsFrame", e)
    class _DocStubsFrame(object): pass


# VertexFrame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from trustedanalytics.core.docstubs1 import _DocStubsVertexFrame
    doc_stubs_import.success(logger, "_DocStubsVertexFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubsVertexFrame", e)
    class _DocStubsVertexFrame(object): pass


# EdgeFrame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from trustedanalytics.core.docstubs1 import _DocStubsEdgeFrame
    doc_stubs_import.success(logger, "_DocStubsEdgeFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "_DocStubsEdgeFrame", e)
    class _DocStubsEdgeFrame(object): pass


@api
@name_support('frame')
class _BaseFrame(_DocStubs_BaseFrame, CommandLoadable):
    _entity_type = 'frame'

    def __init__(self):
        CommandLoadable.__init__(self)

    def __getattr__(self, name):
        """After regular attribute access, try looking up the name of a column.
        This allows simpler access to columns for interactive use."""
        if name == '_backend':
            raise AttributeError('_backend')
        try:
            return super(_BaseFrame, self).__getattribute__(name)
        except AttributeError:
            return self._get_column(name, AttributeError, "Attribute '%s' not found")

    # We are not defining __setattr__.  Columns must be added explicitly

    def __getitem__(self, key):
        if isinstance(key, slice):
            raise TypeError("Slicing not supported")
        return self._get_column(key, KeyError, '%s')

    def _get_column(self, column_name, error_type, error_msg):
        data_type_dict = dict(self.schema)
        try:
            if isinstance(column_name, list):
                return [Column(self, name, data_type_dict[name]) for name in column_name]
            return Column(self, column_name, data_type_dict[column_name])
        except KeyError:
            raise error_type(error_msg % column_name)

    # We are not defining __setitem__.  Columns must be added explicitly

    # We are not defining __delitem__.  Columns must be deleted w/ drop_columns

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(_BaseFrame, self).__repr__() + " (Unable to collect metadata from server)"

    def __len__(self):
        try:
            return len(self.schema)
        except:
            IaError(logger)

    def __contains__(self, key):
        return key in self.column_names  # not very efficient, usage discouraged

    class _FrameIter(object):
        """
        (Private)
        Iterator for Frame - frame iteration works on the columns, returns Column objects
        (see Frame.__iter__)

        Parameters
        ----------
        frame : Frame
            A Frame object.

        """

        def __init__(self, frame):
            self.frame = frame
            # Grab schema once for the duration of the iteration
            # Consider the behavior here --alternative is to ask
            # the backend on each iteration (and there's still a race condition)
            self.schema = frame.schema
            self.i = 0  # the iteration index

        def __iter__(self):
            return self

        def next(self):
            if self.i < len(self.schema):
                column = Column(self.frame, self.schema[self.i][0], self.schema[self.i][1])
                self.i += 1
                return column
            raise StopIteration

    def __iter__(self):
            return Frame._FrameIter(self)

    def __eq__(self, other):
            if not isinstance(other, Frame):
                return False
            return self.uri == other.uri

    def __hash__(self):
        return hash(self.uri)

    @api
    @property
    @returns(list, "list of names of all the frame's columns")
    def __column_names(self):
        """
        Column identifications in the current frame.

        Examples
        --------
        Given a Frame object, *my_frame* accessing a frame.
        To get the column names:

        .. code::

            >>> my_columns = my_frame.column_names
            >>> print my_columns

        Now, given there are three columns *col1*,
        *col2*, and *col3*, the result is:

        .. code::

            ["col1", "col2", "col3"]

        """
        return [name for name, data_type in self._backend.get_schema(self)]

    @api
    @property
    @returns(int, "The number of rows in the frame")
    def __row_count(self):
        """
        Number of rows in the current frame.

        Examples
        --------
        Get the number of rows:

        .. code::

            >>> my_frame.row_count

        The result given is:

        .. code::

            81734

        """
        return self._backend.get_row_count(self, None)

    @api
    @property
    @returns(list, "list of tuples of the form (<column name>, <data type>)")
    def __schema(self):
        """
        Current frame column names and types.

        The schema of the current frame is a list of column names and
        associated data types.
        It is retrieved as a list of tuples.
        Each tuple has the name and data type of one of the frame's columns.

        Examples
        --------
        Given that we have an existing data frame *my_data*, create a Frame,
        then show the frame schema:

        .. code::

            >>> BF = ta.get_frame('my_data')
            >>> print BF.schema

        The result is:

        .. code::

            [("col1", str), ("col2", numpy.int32)]

        """
        return self._backend.get_schema(self)

    @api
    @property
    @returns(data_type=str, description="Status of the frame")
    def __status(self):
        """
        Current frame life cycle status.

        One of three statuses: Active, Deleted, Deleted_Final
           Active:   Frame is available for use
           Deleted:  Frame has been scheduled for deletion can be unscheduled by modifying
           Deleted_Final: Frame's backend files have been removed from disk.

        Examples
        --------
        Given that we have an existing data frame *my_data*, create a Frame,
        then show the frame schema:

        .. code::

            >>> BF = ta.get_frame('my_data')
            >>> print BF.status

        The result is:

        .. code::

            u'Active'
        """
        return self._backend.get_status(self)

    @api
    @has_udf_arg
    @arg('func', 'UDF', "User-Defined Function (|UDF|) which takkes the values in the row and produces a value, or "
         "collection of values, for the new cell(s).")
    @arg('schema', 'tuple | list of tuples', "The schema for the results of the |UDF|, indicating the new column(s) to "
         "add.  Each tuple provides the column name and data type, and is of the form (str, type).")
    @arg('columns_accessed', list, "List of columns which the |UDF| will access.  This adds significant performance "
         "benefit if we know which column(s) will be needed to execute the |UDF|, especially when the frame has "
         "significantly more columns than those being used to evaluate the |UDF|.")
    def __add_columns(self, func, schema, columns_accessed=None):
        """
        Add columns to current frame.

        Assigns data to column based on evaluating a function for each row.

        Notes
        -----
        1)  The row |UDF| ('func') must return a value in the same format as
            specified by the schema.
            See :doc:`/ds_apir`.
        2)  Unicode in column names is not supported and will likely cause the
            drop_frames() method (and others) to fail!

        Examples
        --------
        Given a Frame *my_frame* identifying a data frame with two int32
        columns *column1* and *column2*.
        Add a third column *column3* as an int32 and fill it with the
        contents of *column1* and *column2* multiplied together:

        .. code::

            >>> my_frame.add_columns(lambda row: row.column1*row.column2,
            ... ('column3', int32))

        The frame now has three columns, *column1*, *column2* and *column3*.
        The type of *column3* is an int32, and the value is the product of
        *column1* and *column2*.

        Add a string column *column4* that is empty:

        .. code::

            >>> my_frame.add_columns(lambda row: '', ('column4', str))

        The Frame object *my_frame* now has four columns *column1*, *column2*,
        *column3*, and *column4*.
        The first three columns are int32 and the fourth column is str.
        Column *column4* has an empty string ('') in every row.

        Multiple columns can be added at the same time.
        Add a column *a_times_b* and fill it with the contents of column *a*
        multiplied by the contents of column *b*.
        At the same time, add a column *a_plus_b* and fill it with the contents
        of column *a* plus the contents of column *b*:

        .. only:: html

            .. code::

                >>> my_frame.add_columns(lambda row: [row.a * row.b, row.a + row.b], [("a_times_b", float32), ("a_plus_b", float32))

        .. only:: latex

            .. code::

                >>> my_frame.add_columns(lambda row: [row.a * row.b, row.a +
                ... row.b], [("a_times_b", float32), ("a_plus_b", float32))

        Two new columns are created, "a_times_b" and "a_plus_b", with the
        appropriate contents.

        Given a frame of data and Frame *my_frame* points to it.
        In addition we have defined a |UDF| *func*.
        Run *func* on each row of the frame and put the result in a new int
        column *calculated_a*:

        .. code::

            >>> my_frame.add_columns( func, ("calculated_a", int))

        Now the frame has a column *calculated_a* which has been filled with
        the results of the |UDF| *func*.

        A |UDF| must return a value in the same format as the column is
        defined.
        In most cases this is automatically the case, but sometimes it is less
        obvious.
        Given a |UDF| *function_b* which returns a value in a list, store
        the result in a new column *calculated_b*:

        .. code::

            >>> my_frame.add_columns(function_b, ("calculated_b", float32))

        This would result in an error because function_b is returning a value
        as a single element list like [2.4], but our column is defined as a
        tuple.
        The column must be defined as a list:

        .. code::

            >>> my_frame.add_columns(function_b, [("calculated_b", float32)])

        To run an optimized version of add_columns, columns_accessed parameter can
        be populated with the column names which are being accessed in |UDF|. This
        speeds up the execution by working on only the limited feature set than the
        entire row.

        Let's say a frame has 4 columns named *a*,*b*,*c* and *d* and we want to add a new column
        with value from column *a* multiplied by value in column *b* and call it *a_times_b*.
        In the example below, columns_accessed is a list with column names *a* and *b*.

        .. code::

            >>> my_frame.add_columns(lambda row: row.a * row.b, ("a_times_b", float32), columns_accessed=["a", "b"])

        add_columns would fail if columns_accessed parameter is not populated with the correct list of accessed
        columns. If not specified, columns_accessed defaults to None which implies that all columns might be accessed
        by the |UDF|.

        More information on a row |UDF| can be found at :doc:`/ds_apir`

        """
        # For further examples, see :ref:`example_frame.add_columns`.
        self._backend.add_columns(self, func, schema, columns_accessed)

    @api
    @arg('columns', 'str | list of str | dict', "If not None, the copy will only include the columns specified. "
         "If dict, the string pairs represent a column renaming, {source_column_name: destination_column_name}")
    @arg('where', 'function', "If not None, only those rows for which the UDF evaluates to True will be copied.")
    @arg('name', str, "Name of the copied frame")
    @returns('Frame', "A new Frame of the copied data.")
    def __copy(self, columns=None, where=None, name=None):
        """
        Create new frame from current frame.

        Copy frame or certain frame columns entirely or filtered.
        Useful for frame query.

        Examples
        --------
        Build a Frame from a csv file with 5 million rows of data; call the
        frame "cust":

        .. code::

            >>> my_frame = ta.Frame(source="my_data.csv")
            >>> my_frame.name("cust")

        Given the frame has columns *id*, *name*, *hair*, and *shoe*.
        Copy it to a new frame:

        .. code::

            >>> your_frame = my_frame.copy()

        Now we have two frames of data, each with 5 million rows.
        Checking the names:

        .. code::

            >>> print my_frame.name()
            >>> print your_frame.name()

        Gives the results:

        .. code::

            "cust"
            "frame_75401b7435d7132f5470ba35..."

        Now, let's copy *some* of the columns from the original frame:

        .. code::

            >>> our_frame = my_frame.copy(['id', 'hair'])

        Our new frame now has two columns, *id* and *hair*, and has 5 million
        rows.
        Let's try that again, but this time change the name of the *hair*
        column to *color*:

        .. code::

            >>> last_frame = my_frame.copy(('id': 'id', 'hair': 'color'))

        """
        return self._backend.copy(self, columns, where, name)

    @api
    @arg('where', 'function', "|UDF| which evaluates a row to a boolean")
    @returns(int, "number of rows for which the where |UDF| evaluated to True.")
    def __count(self, where):
        """Counts the number of rows which meet given criteria."""
        return self._backend.get_row_count(self, where)

    @api
    @arg('n', int, 'The number of rows to download to the client')
    @arg('offset', int, 'The number of rows to skip before copying')
    @arg('columns', list, 'Column filter, the names of columns to be included (default is all columns)')
    @returns('pandas.DataFrame', 'A new pandas dataframe object containing the downloaded frame data' )
    def __download(self, n=100, offset=0, columns=None):
        """
        Download a frame from the server into client workspace.

        Copies an trustedanalytics Frame into a Pandas DataFrame.


        Examples
        --------
        Frame *my_frame* accesses a frame with millions of rows of data.
        Get a sample of 500 rows:

        .. code::

            >>> pandas_frame = my_frame.download( 500 )

        We now have a new frame accessed by a pandas DataFrame *pandas_frame*
        with a copy of the first 500 rows of the original frame.

        If we use the method with an offset like:

        .. code::

            >>> pandas_frame = my_frame.take( 500, 100 )

        We end up with a new frame accessed by the pandas DataFrame
        *pandas_frame* again, but this time it has a copy of rows 101 to 600 of
        the original frame.

        """
        try:
            import pandas
        except:
            raise RuntimeError("pandas module not found, unable to download.  Install pandas or try the take command.")
        from trustedanalytics.core.atkpandas import atk_dtype_to_pandas_str
        result = self._backend.take(self, n, offset, columns)
        headers, data_types = zip(*result.schema)

        pandas_df = pandas.DataFrame(result.data, columns=headers)

        for i, dtype in enumerate(data_types):
            dtype_str = atk_dtype_to_pandas_str(dtype)
            pandas_df[[headers[i]]] = pandas_df[[headers[i]]].astype(dtype_str)
        return pandas_df

    @api
    @has_udf_arg
    @arg('predicate', 'function', "|UDF| which evaluates a row to a boolean; rows that answer True are dropped from the Frame")
    def __drop_rows(self, predicate):
        """
        Erase any row in the current frame which qualifies.

        Examples
        --------
        For this example, my_frame is a Frame object accessing a frame with
        lots of data for the attributes of ``lions``, ``tigers``, and
        ``ligers``.
        Get rid of the ``lions`` and ``tigers``:

        .. code::

            >>> my_frame.drop_rows(lambda row: row.animal_type == "lion" or
            ...    row.animal_type == "tiger")

        Now the frame only has information about ``ligers``.

        More information on a |UDF| can be found at :doc:`/ds_apir`.


        """
        # For further examples, see :ref:`example_frame.drop_rows`
        self._backend.drop(self, predicate)

    @api
    @has_udf_arg
    @arg('predicate', 'function', "|UDF| which evaluates a row to a boolean; rows that answer False are dropped from the Frame")
    def __filter(self, predicate):
        """
        Select all rows which satisfy a predicate.

        Modifies the current frame to save defined rows and delete everything
        else.

        Examples
        --------
        For this example, *my_frame* is a Frame object with lots of data for
        the attributes of ``lizards``, ``frogs``, and ``snakes``.
        Get rid of everything, except information about ``lizards`` and
        ``frogs``:

        .. code::

            >>> def my_filter(row):
            ... return row['animal_type'] == 'lizard' or
            ... row['animal_type'] == "frog"

            >>> my_frame.filter(my_filter)

        The frame now only has data about ``lizards`` and ``frogs``.

        More information on a |UDF| can be found at :doc:`/ds_apir`.

        """
        # For further examples, see :ref:`example_frame.filter`
        self._backend.filter(self, predicate)

    @api
    def __get_error_frame(self):
        """
        Get a frame with error recordings.

        When a frame is created, another frame is transparently
        created to capture parse errors.

        Returns
        -------
        Frame : error frame object
            A new object accessing a frame that contains the parse errors of
            the currently active Frame or None if no error frame exists.
        """
        return self._backend.get_error_frame(self)

    @api
    @beta
    @arg('group_by_columns', list, 'Column name or list of column names')
    @arg('aggregation_arguments', dict, "Aggregation function based on entire row, and/or dictionaries (one or more) of { column name str : aggregation function(s) }.")
    @returns('Frame', 'A new frame with the results of the group_by')
    def __group_by(self, group_by_columns, *aggregation_arguments):
        """
        Create summarized frame.

        Creates a new frame and returns a Frame object to access it.
        Takes a column or group of columns, finds the unique combination of
        values, and creates unique rows with these column values.
        The other columns are combined according to the aggregation
        argument(s).

        Notes
        -----
        *   Column order is not guaranteed when columns are added
        *   The column names created by aggregation functions in the new frame
            are the original column name appended with the '_' character and
            the aggregation function.
            For example, if the original field is *a* and the function is
            *avg*, the resultant column is named *a_avg*.
        *   An aggregation argument of *count* results in a column named
            *count*.
        *   The aggregation function *agg.count* is the only full row
            aggregation function supported at this time.
        *   Aggregation currently supports using the following functions:

            *   avg
            *   count
            *   count_distinct
            *   max
            *   min
            *   stdev
            *   sum
            *   var (see glossary :term:`Bias vs Variance`)

        Examples
        --------
        For setup, we will use a Frame *my_frame* accessing a frame with a
        column *a*:

        .. code::

            >>> my_frame.inspect()

              a:str
            /-------/
              cat
              apple
              bat
              cat
              bat
              cat

        Create a new frame, combining similar values of column *a*,
        and count how many of each value is in the original frame:

        .. code::

            >>> new_frame = my_frame.group_by('a', agg.count)
            >>> new_frame.inspect()

              a:str       count:int
            /-----------------------/
              cat             3
              apple           1
              bat             2

        In this example, 'my_frame' is accessing a frame with three columns,
        *a*, *b*, and *c*:

        .. code::

            >>> my_frame.inspect()

              a:int   b:str   c:float
            /-------------------------/
              1       alpha     3.0
              1       bravo     5.0
              1       alpha     5.0
              2       bravo     8.0
              2       bravo    12.0

        Create a new frame from this data, grouping the rows by unique
        combinations of column *a* and *b*.
        Average the value in *c* for each group:

        .. code::

            >>> new_frame = my_frame.group_by(['a', 'b'], {'c' : agg.avg})
            >>> new_frame.inspect()

              a:int   b:str   c_avg:float
            /-----------------------------/
              1       alpha     4.0
              1       bravo     5.0
              2       bravo    10.0

        For this example, we use *my_frame* with columns *a*, *c*, *d*,
        and *e*:

        .. code::

            >>> my_frame.inspect()

              a:str   c:int   d:float e:int
            /-------------------------------/
              ape     1       4.0     9
              ape     1       8.0     8
              big     1       5.0     7
              big     1       6.0     6
              big     1       8.0     5

        Create a new frame from this data, grouping the rows by unique
        combinations of column *a* and *c*.
        Count each group; for column *d* calculate the average, sum and minimum
        value.
        For column *e*, save the maximum value:

        .. only:: html

            .. code::

                >>> new_frame = my_frame.group_by(['a', 'c'], agg.count, {'d': [agg.avg, agg.sum, agg.min], 'e': agg.max})

                  a:str   c:int   count:int  d_avg:float  d_sum:float   d_min:float   e_max:int
                /-------------------------------------------------------------------------------/
                  ape     1       2          6.0          12.0          4.0           9
                  big     1       3          6.333333     19.0          5.0           7

        .. only:: latex

            .. code::

                >>> new_frame = my_frame.group_by(['a', 'c'], agg.count,
                ... {'d': [agg.avg, agg.sum, agg.min], 'e': agg.max})

                  a    c    count  d_avg  d_sum  d_min  e_max
                  str  int  int    float  float  float  int
                /---------------------------------------------/
                  ape  1    2      6.0    12.0   4.0    9
                  big  1    3      6.333  19.0   5.0    7


        For further examples, see :ref:`example_frame.group_by`.
        """
        return self._backend.group_by(self, group_by_columns, aggregation_arguments)

    @api
    @alpha
    @arg('column_inputs', 'str | tuple(str, dict)', 'Comma-separated column names to summarize or tuple containing column name '
                                                    'and dictionary of optional parameters. '
                                                    'Optional parameters (see below for details): '
                                                    'top_k (default = 10), threshold (default = 0.0)')

    @returns(dict, 'Summary for specified column(s) consisting of levels with their frequency and percentage')
    def __categorical_summary(self, *column_inputs):
        """
        Compute a summary of the data in a column(s) for categorical or numerical data types.
        The returned value is a Map containing categorical summary for each specified column.

        For each column, levels which satisfy the top k and/or threshold cutoffs are displayed along
        with their frequency and percentage occurrence with respect to the total rows in the dataset.

        Missing data is reported when a column value is empty ("") or null.

        All remaining data is grouped together in the Other category and its frequency and percentage are reported as well.

        User must specify the column name and can optionally specify top_k and/or threshold.

        Optional parameters:

            top_k
                Displays levels which are in the top k most frequently occurring values for that column.

            threshold
                Displays levels which are above the threshold percentage with respect to the total row count.

            top_k and threshold
                Performs level pruning first based on top k and then filters out levels which satisfy the threshold criterion.

            defaults
                Displays all levels which are in Top 10.


        Examples
        --------

        .. code::

            >>> frame.categorical_summary('source','target')
            >>> frame.categorical_summary(('source', {'top_k' : 2}))
            >>> frame.categorical_summary(('source', {'threshold' : 0.5}))
            >>> frame.categorical_summary(('source', {'top_k' : 2}), ('target',
            ... {'threshold' : 0.5}))

        Sample output (for last example above):

            >>> {u'categorical_summary': [{u'column': u'source', u'levels': [
            ... {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'thing'},
            ... {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'abstraction'},
            ... {u'percentage': 0.25, u'frequency': 7, u'level': u'physical_entity'},
            ... {u'percentage': 0.10714285714285714, u'frequency': 3, u'level': u'entity'},
            ... {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
            ... {u'percentage': 0.0, u'frequency': 0, u'level': u'Other'}]},
            ... {u'column': u'target', u'levels': [
            ... {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'thing'},
            ... {u'percentage': 0.07142857142857142, u'frequency': 2,
            ...  u'level': u'physical_entity'},
            ... {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'entity'},
            ... {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'variable'},
            ... {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'unit'},
            ... {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'substance'},
            ... {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'subject'},
            ... {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'set'},
            ... {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'reservoir'},
            ... {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'relation'},
            ... {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
            ... {u'percentage': 0.5357142857142857, u'frequency': 15, u'level': u'Other'}]}]}

        """
        return self._backend.categorical_summary(self, column_inputs)

    @api
    @arg('n', int, 'The number of rows to print.')
    @arg('offset', int, 'The number of rows to skip before printing.')
    @arg('columns', int, 'Filter columns to be included.  By default, all columns are included')
    @arg('wrap', "int or 'stripes'", "If set to 'stripes' then inspect prints rows in stripes; if set to an integer N, "
                                     "rows will be printed in clumps of N columns, where the columns are wrapped")
    @arg('truncate', int, 'If set to integer N, all strings will be truncated to length N, including a tagged ellipses')
    @arg('round', int, 'If set to integer N, all floating point numbers will be rounded and truncated to N digits')
    @arg('width', int, 'If set to integer N, the print out will try to honor a max line width of N')
    @arg('margin', int, "('stripes' mode only) If set to integer N, the margin for printing names in a "
                        "stripe will be limited to N characters")
    @arg('with_types', bool, "If set to True, header will include the data_type of each column")
    @returns('RowsInspection', "An object which naturally converts to a pretty-print string")
    def __inspect(self,
                  n=10,
                  offset=0,
                  columns=None,
                  wrap=inspect_settings._unspecified,
                  truncate=inspect_settings._unspecified,
                  round=inspect_settings._unspecified,
                  width=inspect_settings._unspecified,
                  margin=inspect_settings._unspecified,
                  with_types=inspect_settings._unspecified):
        """
        Pretty-print of the frame data

        Essentially returns a string, but technically returns a RowInspection object which renders a string.
        The RowInspection object naturally converts to a str when needed, like when printed or when displayed
        by python REPL (i.e. using the object's __repr__).  If running in a script and want the inspect output
        to be printed, then it must be explicitly printed, then `print frame.inspect()`

        Examples
        --------
        Given a frame of data and a Frame to access it.
        To look at the first 4 rows of data:

        .. code::

            >>> my_frame.inspect(4)
           [#]    animal      name    age     weight
           =========================================
           [0]  human       George      8      542.5
           [1]  human       Ursula      6      495.0
           [2]  ape         Ape        41      400.0
           [3]  elephant    Shep        5     8630.0

        # For other examples, see :ref:`example_frame.inspect`.

        **Global Settings**

        If not specified, the arguments that control formatting receive default values from
        'trustedanalytics.inspect_settings'.  Make changes there to affect all calls to inspect.

        .. code::

            >>> import trustedanalytics as ta
            >>> ta.inspect_settings
            wrap             20
            truncate       None
            round          None
            width            80
            margin         None
            with_types    False
            >>> ta.inspect_settings.width = 120  # changes inspect to use 120 width globally
            >>> ta.inspect_settings.truncate = 16  # changes inspect to always truncate strings to 16 chars
            >>> ta.inspect_settings
            wrap             20
            truncate         16
            round          None
            width           120
            margin         None
            with_types    False
            >>> ta.inspect_settings.width = None  # return value back to default
            >>> ta.inspect_settings
            wrap             20
            truncate         16
            round          None
            width            80
            margin         None
            with_types    False
            >>> ta.inspect_settings.reset()  # set everything back to default
            >>> ta.inspect_settings
            wrap             20
            truncate       None
            round          None
            width            80
            margin         None
            with_types    False

        ..
        """
        format_settings = inspect_settings.copy(wrap, truncate, round, width, margin, with_types)
        return self._backend.inspect(self, n, offset, columns, format_settings=format_settings)

    @api
    @beta
    @arg('right', 'Frame', "Another frame to join with")
    @arg('left_on', str, "Name of the column in the left frame used to match up the two frames.")
    @arg('right_on', str, "Name of the column in the right frame used to match up the two frames. "
         "Default is the same as the left frame.")
    @arg('how', str, "How to qualify the data to be joined together.  Must be one of the following:  "
         "'left', 'right', 'inner', 'outer'.  Default is 'inner'")
    @arg('name', str, "Name of the result grouped frame")
    @returns('Frame', 'A new frame with the results of the join')
    def __join(self, right, left_on, right_on=None, how='inner', name=None):
        """
        Join operation on one or two frames, creating a new frame.

        Create a new frame from a SQL JOIN operation with another frame.
        The frame on the 'left' is the currently active frame.
        The frame on the 'right' is another frame.
        This method takes a column in the left frame and matches its values
        with a column in the right frame.
        Using the default 'how' option ['inner'] will only allow data in the
        resultant frame if both the left and right frames have the same value
        in the matching column.
        Using the 'left' 'how' option will allow any data in the resultant
        frame if it exists in the left frame, but will allow any data from the
        right frame if it has a value in its column which matches the value in
        the left frame column.
        Using the 'right' option works similarly, except it keeps all the data
        from the right frame and only the data from the left frame when it
        matches.
        The 'outer' option provides a frame with data from both frames where
        the left and right frames did not have the same value in the matching
        column.

        Notes
        -----
        When a column is named the same in both frames, it will result in two
        columns in the new frame.
        The column from the *left* frame (originally the current frame) will be
        copied and the column name will have the string "_L" added to it.
        The same thing will happen with the column from the *right* frame,
        except its name has the string "_R" appended. The order of columns
        after this method is called is not guaranteed.

        It is recommended that you rename the columns to meaningful terms prior
        to using the ``join`` method.
        Keep in mind that unicode in column names will likely cause the
        drop_frames() method (and others) to fail!

        Examples
        --------
        For this example, we will use a Frame *my_frame* accessing a frame with
        columns *a*, *b*, *c*, and a Frame *your_frame* accessing a frame with
        columns *a*, *d*, *e*.
        Join the two frames keeping only those rows having the same value in
        column *a*:

        .. code::

            >>> print my_frame.inspect()

              a:unicode   b:unicode   c:unicode
            /--------------------------------------/
              alligator   bear        cat
              apple       berry       cantaloupe
              auto        bus         car
              mirror      frog        ball

            >>> print your_frame.inspect()

              b:unicode   c:int   d:unicode
            /-------------------------------------/
              berry        5218   frog
              blue            0   log
              bus           871   dog

            >>> joined_frame = my_frame.join(your_frame, 'b', how='inner')

        Now, joined_frame is a Frame accessing a frame with the columns *a*,
        *b*, *c_L*, *ci_R*, and *d*.
        The data in the new frame will be from the rows where column 'a' was
        the same in both frames.

        .. code::

            >>> print joined_frame.inspect()

              a:unicode   b:unicode     c_L:unicode   c_R:int64   d:unicode
            /-------------------------------------------------------------------/
              apple       berry         cantaloupe         5218   frog
              auto        bus           car                 871   dog

        More examples can be found in the :ref:`user manual
        <example_frame.join>`.

        """
        return self._backend.join(self, right, left_on, right_on, how, name)

    @api
    @beta
    @arg('columns', 'str | list of str | list of tuples', "Either a column name, a list of column names, or a "
         "list of tuples where each tuple is a name and an ascending bool value.")
    @arg('ascending', bool, "True for ascending, False for descending.")
    def __sort(self, columns, ascending=True):
        """
        Sort the data in a frame.

        Sort a frame by column values either ascending or descending.

        Examples
        --------
        Sort a single column:

        .. code::

            >>> frame.sort('column_name')

        Sort a single column ascending:

        .. code::

            >>> frame.sort('column_name', True)

        Sort a single column descending:

        .. code::

            >>> frame.sort('column_name', False)

        Sort multiple columns:

        .. code::

            >>> frame.sort(['col1', 'col2'])

        Sort multiple columns ascending:

        .. code::

            >>> frame.sort(['col1', 'col2'], True)

        Sort multiple columns descending:

        .. code::

            >>> frame.sort(['col1', 'col2'], False)

        Sort multiple columns: 'col1' ascending and 'col2' descending:

        .. code::

            >>> frame.sort([ ('col1', True), ('col2', False) ])

        """
        return self._backend.sort(self, columns, ascending)

    @api
    @arg('n', int, "The number of rows to copy to the client from the frame.")
    @arg('offset', int, "The number of rows to skip before starting to copy")
    @arg('columns', 'str | iterable of str', "If not None, only the given columns' data will be provided.  "
         "By default, all columns are included")
    @returns(list, "A list of lists, where each contained list is the data for one row.")
    def __take(self, n, offset=0, columns=None):
        """
        Get data subset.

        Take a subset of the currently active Frame.

        Notes
        -----
        The data is considered 'unstructured', therefore taking a certain
        number of rows, the rows obtained may be different every time the
        command is executed, even if the parameters do not change.

        Examples
        --------
        Frame *my_frame* accesses a frame with millions of rows of data.
        Get a sample of 5000 rows:

        .. code::

            >>> my_data_list = my_frame.take( 5000 )

        We now have a list of data from the original frame.

        .. code::

            >>> print my_data_list

            [[ 1, "text", 3.1415962 ]
             [ 2, "bob", 25.0 ]
             [ 3, "weave", .001 ]
             ...]

        If we use the method with an offset like:

        .. code::

            >>> my_data_list = my_frame.take( 5000, 1000 )

        We end up with a new list, but this time it has a copy of the data from
        rows 1001 to 5000 of the original frame.

        """
        result = self._backend.take(self, n, offset, columns)
        return result.data



@api
class Frame(_DocStubsFrame, _BaseFrame):
    """Large table of data.

Class with information about a large row and columnar data store in a frame,
Has information needed to modify data and table structure."""
    @api
    @arg('source', 'CsvFile | Frame', "A source of initial data.")
    @arg('name', str, """The name of the newly created frame.
Default is None.""")
    def __init__(self, source=None, name=None, _info=None):
        """
    Create a Frame/frame.

    Notes
    -----
    A frame with no name is subject to garbage collection.

    If a string in the CSV file starts and ends with a double-quote (")
    character, the character is stripped off of the data before it is put into
    the field.
    Anything, including delimiters, between the double-quote characters is
    considered part of the str.
    If the first character after the delimiter is anything other than a
    double-quote character, the string will be composed of all the characters
    between the delimiters, including double-quotes.
    If the first field type is str, leading spaces on each row are
    considered part of the str.
    If the last field type is str, trailing spaces on each row are
    considered part of the str.

    Examples
    --------
    Create a new frame based upon the data described in the CsvFile object
    *my_csv_schema*.
    Name the frame "myframe".
    Create a Frame *my_frame* to access the data:

    .. code::

        >>> my_frame = ta.Frame(my_csv_schema, "myframe")

    A Frame object has been created and *my_frame* is its proxy.
    It brought in the data described by *my_csv_schema*.
    It is named *myframe*.

    Create an empty frame; name it "yourframe":

    .. code::

        >>> your_frame = ta.Frame(name='yourframe')

    A frame has been created and Frame *your_frame* is its proxy.
    It has no data yet, but it does have the name *yourframe*.
        """
        self.uri = None
        with api_context(logger, 3, self.__init__, self, source, name, _info):
            api_status.verify_installed()
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            _BaseFrame.__init__(self)
            new_frame_name = self._backend.create(self, source, name, _info)
            logger.info('Created new frame "%s"', new_frame_name)

    @api
    @arg('data', 'Data source', "Data source, see :doc:`Data Sources </python_api/datasources/index>`")
    def __append(self, data):
        """
        Adds more data to the current frame.

        Examples
        --------
        Given a frame with a single column, *col_1*:

        .. code::

                >>> my_frame.inspect(4)
                  col_1:str
                /-----------/
                  dog
                  cat
                  bear
                  donkey

          and a frame with two columns, *col_1* and *col_2*:

          ..code::

                >>> your_frame.inspect(4)
                  col_1:str  col_qty:int32
                /--------------------------/
                  bear          15
                  cat            2
                  snake          8
                  horse          5

        Column *col_1* means the same thing in both frames.
        The Frame *my_frame* points to the first frame and *your_frame* points
        to the second.
        To add the contents of *your_frame* to *my_frame*:

        .. code::

            >>> my_frame.append(your_frame)
            >>> my_frame.inspect(8)
              col_1:str  col_2:int32
            /------------------------/
              dog           None
              bear            15
              bear          None
              horse            5
              cat           None
              cat              2
              donkey        None
              snake            5

        Now the first frame has two columns, *col_1* and *col_2*.
        Column *col_1* has the data from *col_1* in both original frames.
        Column *col_2* has None (undefined) in all of the rows in the original
        first frame, and has the value of the second frame column, *col_2*, in
        the rows matching the new data in *col_1*.

        Breaking it down differently, the original rows referred to by
        *my_frame* have a new column, *col_2*, and this new column is filled
        with non-defined data.
        The frame referred to by *your_frame*, is then added to the bottom.

        """
        self._backend.append(self, data)


@api
class VertexFrame(_DocStubsVertexFrame, _BaseFrame):
    """A list of Vertices owned by a Graph..

A VertexFrame is similar to a Frame but with a few important differences:

-   VertexFrames are not instantiated directly by the user, instead they
    are created by defining a vertex type in a graph
-   Each row of a VertexFrame represents a vertex in a graph
-   VertexFrames have many of the same methods as Frames but not all (for
    example, flatten_column())
-   VertexFrames have extra methods not found on Frames (for example,
    add_vertices())
-   Removing a vertex (or row) from a VertexFrame also removes edges
    connected to that vertex from the graph
-   VertexFrames have special system columns (_vid, _label) that are
    maintained automatically by the system and cannot be modified by the
    user
-   VertexFrames have a special user defined id column whose value uniquely
    identifies the vertex
-   "Columns" on a VertexFrame can also be thought of as "properties" on
    vertices
    """
    # For other examples, see :ref:`example_frame.frame`.

    # TODO - Review Parameters, Examples

    _entity_type = 'frame:vertex'

    @api
    def __init__(self, source=None, graph=None, label=None, _info=None):
        """
    Examples
    --------
    Given a data file, create a frame, move the data to graph and then define a
    new VertexFrame and add data to it:

    .. only:: html

        .. code::

            >>> csv = ta.CsvFile("/movie.csv", schema= [('user_id', int32), ('user_name', str), ('movie_id', int32), ('movie_title', str), ('rating', str)])
            >>> my_frame = ta.Frame(csv)
            >>> my_graph = ta.Graph()
            >>> my_graph.define_vertex_type('users')
            >>> my_vertex_frame = my_graph.vertices['users']
            >>> my_vertex_frame.add_vertices(my_frame, 'user_id', ['user_name', 'age'])

    .. only:: html

        .. code::

            >>> csv = ta.CsvFile("/movie.csv", schema= [('user_id', int32),
            ...                                     ('user_name', str),
            ...                                     ('movie_id', int32),
            ...                                     ('movie_title', str),
            ...                                     ('rating', str)])
            >>> my_frame = ta.Frame(csv)
            >>> my_graph = ta.Graph()
            >>> my_graph.define_vertex_type('users')
            >>> my_vertex_frame = my_graph.vertices['users']
            >>> my_vertex_frame.add_vertices(my_frame, 'user_id',
            ... ['user_name', 'age'])

    Retrieve a previously defined graph and retrieve a VertexFrame from it:

    .. code::

        >>> my_graph = ta.get_graph("your_graph")
        >>> my_vertex_frame = my_graph.vertices["your_label"]

    Calling methods on a VertexFrame:

    .. code::

        >>> my_vertex_frame.vertices["your_label"].inspect(20)

    Convert a VertexFrame to a frame:

    .. code::

        >>> new_Frame = my_vertex_frame.vertices["label"].copy()
        """
        try:
            api_status.verify_installed()
            self.uri = None
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            _BaseFrame.__init__(self)
            new_frame_name = self._backend.create_vertex_frame(self, source, label, graph, _info)
            logger.info('Created new vertex frame "%s"', new_frame_name)
        except:
            error = IaError(logger)
            raise error

    @api
    @arg('predicate', 'function', "|UDF| which evaluates a row (vertex) to a boolean; vertices that answer True are dropped from the Frame")
    def __drop_vertices(self, predicate):
        """
        Delete rows that qualify.

        Parameters
        ----------
        predicate : |UDF|
            |UDF| or :term:`lambda` which takes a row argument and evaluates
            to a boolean value.

        Examples
        --------
        Given VertexFrame object *my_vertex_frame* accessing a graph with lots
        of data for the attributes of ``lions``, ``tigers``, and ``ligers``.
        Get rid of the ``lions`` and ``tigers``:

        .. only:: html

            .. code::

                >>> my_vertex_frame.drop_vertices(lambda row: row.animal_type == "lion" or row.animal_type == "tiger")

        .. only:: latex

            .. code::

                >>> my_vertex_frame.drop_vertices(lambda row:
                ...     row.animal_type == "lion" or
                ...     row.animal_type == "tiger")

        Now the frame only has information about ``ligers``.

        More information on |UDF| can be found at :doc:`/ds_apir`

        """
        self._backend.filter_vertices(self, predicate, keep_matching_vertices=False)

    @api
    @arg('predicate', 'function', "|UDF| which evaluates a row to a boolean; vertices that answer False are dropped from the Frame")
    def __filter(self, predicate):
        self._backend.filter_vertices(self, predicate)


@api
class EdgeFrame(_DocStubsEdgeFrame, _BaseFrame):
    """A list of Edges owned by a Graph.

An EdgeFrame is similar to a Frame but with a few important differences:

-   EdgeFrames are not instantiated directly by the user, instead they are
    created by defining an edge type in a graph
-   Each row of an EdgeFrame represents an edge in a graph
-   EdgeFrames have many of the same methods as Frames but not all
-   EdgeFrames have extra methods not found on Frames (e.g. add_edges())
-   EdgeFrames have a dependency on one or two VertexFrames
    (adding an edge to an EdgeFrame requires either vertices to be present
    or for the user to specify create_missing_vertices=True)
-   EdgeFrames have special system columns (_eid, _label, _src_vid,
    _dest_vid) that are maintained automatically by the system and cannot
    be modified by the user
-   "Columns" on an EdgeFrame can also be thought of as "properties" on
    Edges
    """

    _entity_type = 'frame:edge'

    @api
    @arg('graph','?',"""graph these edges belong to""")
    @arg('label','?',"""edge label""")
    @arg('src_vertex_label','?',"""label of the source vertex type""")
    @arg('dest_vertex_label','?',"""label of the destination vertex type""")
    @arg('directed','?',"""directed or undirected""")
    @arg('_info','?',"""""")
    @returns('VertexFrame object',"""An object with access to the frame.""")
    def __init__(self, graph=None, label=None, src_vertex_label=None, dest_vertex_label=None, directed=None, _info=None):
        """

    Examples
    --------
    Given a data file */movie.csv*, create a frame to match this data and move
    the data to the frame.
    Create an empty graph and define some vertex and edge types.

    .. code::

        >>> my_csv = ta.CsvFile("/movie.csv", schema= [('user_id', int32),
        ...                                     ('user_name', str),
        ...                                     ('movie_id', int32),
        ...                                     ('movie_title', str),
        ...                                     ('rating', str)])

        >>> my_frame = ta.Frame(my_csv)
        >>> my_graph = ta.Graph()
        >>> my_graph.define_vertex_type('users')
        >>> my_graph.define_vertex_type('movies')
        >>> my_graph.define_edge_type('ratings','users','movies',directed=True)

    Add data to the graph from the frame:

    .. only:: html

        .. code::

            >>> my_graph.vertices['users'].add_vertices(my_frame, 'user_id', ['user_name'])
            >>> my_graph.vertices['movies].add_vertices(my_frame, 'movie_id', ['movie_title])

    .. only:: latex

        .. code::

            >>> my_graph.vertices['users'].add_vertices(my_frame, 'user_id',
            ... ['user_name'])
            >>> my_graph.vertices['movies'].add_vertices(my_frame, 'movie_id', ['movie_title'])

    Create an edge frame from the graph, and add edge data from the frame.

    .. code::

        >>> my_edge_frame = graph.edges['ratings']
        >>> my_edge_frame.add_edges(my_frame, 'user_id', 'movie_id', ['rating']

    Retrieve a previously defined graph and retrieve an EdgeFrame from it:

    .. code::

        >>> my_old_graph = ta.get_graph("your_graph")
        >>> my_new_edge_frame = my_old_graph.edges["your_label"]

    Calling methods on an EdgeFrame:

    .. code::

        >>> my_new_edge_frame.inspect(20)

    Copy an EdgeFrame to a frame using the copy method:

    .. code::

        >>> my_new_frame = my_new_edge_frame.copy()
        """
        try:
            api_status.verify_installed()
            self.uri = None
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            _BaseFrame.__init__(self)
            new_frame_name = self._backend.create_edge_frame(self, label, graph, src_vertex_label, dest_vertex_label, directed, _info)
            logger.info('Created new edge frame "%s"', new_frame_name)
        except:
            error = IaError(logger)
            raise error
