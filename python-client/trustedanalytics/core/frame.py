# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
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

"""
<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-

# Note: This frame is going to be used throughout this file's examples

>>> _name = "example_frame"

>>> try:
...     ta.drop_frames(_name)
... except:
...     pass
-etc-

>>> _schema = [('name',str), ('age', int), ('tenure', int), ('phone', str)]
>>> _rows = [['Fred', 39, 16, '555-1234'], ['Susan', 33, 3, '555-0202'], ['Thurston', 65, 26, '555-4510'], ['Judy', 44, 14, '555-2183']]
>>> _frame = ta.Frame(ta.UploadRows(_rows, _schema))
-etc-

</hide>
"""


def _get_backend():
    from trustedanalytics.meta.config import get_frame_backend
    return get_frame_backend()

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
@arg("*items", "List of strings (frame, graph, or model name) or proxy objects (the frame, graph, or model object itself).", "Deletes the specified frames, graphs, and models from the server.")
@returns(int, 'Number of items deleted.' )
def __drop(*items):
    """
    drop() serves as an alias to drop_frames, drop_graphs, and drop_models.

    It accepts multiple parameters, which can contain strings (the name of the frame, graph, or model),
    proxy objects (the frame, graph, or model object itself), or a list of strings and/or proxy objects.
    If the item provided is a string and no frame, graph, or model is found with the specified name,
    no action is taken.

    If the item type is not recognized (not a string, frame, graph, or model) an ArgumentError is raised.

    Examples
    --------

    Given a frame, model, and graph like:

        .. code::

            >>> my_frame = ta.Frame()

            >>> my_model = ta.KMeansModel()
            <progress>

            >>> my_graph = ta.Graph()
            -etc-

    The drop() command can be used to delete the frame, model, and graph from the server.  It returns the number
    of items that have been deleted.

        .. code::

            >>> ta.drop(my_frame, my_model, my_graph)
            3

    Alternatively, we can pass the object's string name to drop() like:

    .. code::

            >>> my_frame = ta.Frame(name='example_frame')

            >>> ta.drop('example_frame')
            1

    """
    from trustedanalytics import drop_frames, drop_graphs, drop_models, _BaseGraph, _BaseModel, get_frame_names, get_graph_names, get_model_names

    num_deleted = 0     # track the number of items we delete to return

    # Flatten out the items (break out lists)
    flat_item_list = []

    for item in items:
        if isinstance(item, list):
            flat_item_list.extend(item)
        else:
            flat_item_list.append(item)

    for item in flat_item_list:
        if item is not None:
            if isinstance(item, basestring):
                # If the item is a string, try calling drop_* functions, until we're successful.
                item_name = str(item)

                temp_drop_count = drop_frames(item)

                if temp_drop_count == 0:
                    temp_drop_count = drop_graphs(item)

                if temp_drop_count == 0:
                    temp_drop_count = drop_models(item)

                # Note that if the item_name is not found in frames, graphs, or models, we intentionally do not fail

                num_deleted += temp_drop_count
            else:
                # If the item isn't a string, check the object type to call the appropriate drop_* function
                if isinstance(item, _BaseFrame):
                    num_deleted += drop_frames(item)
                elif isinstance(item, _BaseGraph):
                    num_deleted += drop_graphs(item)
                elif isinstance(item, _BaseModel):
                    num_deleted += drop_models(item)
                else:
                    # Unsupported object type passed to drop(), raise an exception
                    raise AttributeError("Unsupported item type: {0}".format(type(item)))

    return num_deleted

@api
@name_support('frame')
class _BaseFrame(CommandLoadable):
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

        Returns the names of the columns of the current frame.

        Examples
        --------

        .. code::

            <hide>
            >>> frame = _frame.copy(name=_name)
            -etc-

            </hide>

            >>> frame.column_names
            [u'name', u'age', u'tenure', u'phone']

        """
        return [name for name, data_type in self._backend.get_schema(self)]

    @api
    @property
    @returns(int, "The number of rows in the frame")
    def __row_count(self):
        """
        Number of rows in the current frame.

        Counts all of the rows in the frame.

        Examples
        --------
        Get the number of rows:

        .. code::

            >>> frame.row_count
            4

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

        .. code::

            >>> frame.schema
            [(u'name', <type 'unicode'>), (u'age', <type 'numpy.int32'>), (u'tenure', <type 'numpy.int32'>), (u'phone', <type 'unicode'>)]

        Note how the types shown are the raw, underlying types used in python.  To see the schema in a friendlier
        format, used the __repr__ presentation, invoke by simply entering the frame:
            >>> frame
            Frame "example_frame"
            row_count = 4
            schema = [name:unicode, age:int32, tenure:int32, phone:unicode]
            status = ACTIVE  (last_read_date = -etc-)

        """
        return self._backend.get_schema(self)

    @api
    @property
    @returns(data_type=str, description="Status of the frame")
    def __status(self):
        """
        Current frame life cycle status.

        One of three statuses: ACTIVE, DROPPED, FINALIZED
           ACTIVE:    Entity is available for use
           DROPPED:   Entity has been dropped by user or by garbage collection which found it stale
           FINALIZED: Entity's data has been deleted

        Examples
        --------

        .. code::

            >>> frame.status
            u'ACTIVE'

        """
        return self._backend.get_status(self)

    @api
    @property
    @returns(data_type=str, description="Date string of the last time this frame's data was accessed")
    def __last_read_date(self):
        """
        Last time this frame's data was accessed.

        Examples
        --------

        .. code::

        <hide>
            # for doctest we'll at least call the property, but we won't compare its results exactly
            >>> frame.last_read_date
            -etc-

        </hide>
        <skip>
            >>> frame.last_read_date
            datetime.datetime(2015, 10, 8, 15, 48, 8, 791000, tzinfo=tzoffset(None, -25200))

        </skip>
        """
        return self._backend.get_last_read_date(self)

    @api
    @has_udf_arg
    @arg('func', 'UDF', "User-Defined Function (|UDF|) which takes the values in the row and produces a value, or "
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
        Given our frame, let's add a column which has how many years the person has been over 18

        .. code::

            <hide>
            >>> frame = _frame.copy()
            -etc-

            </hide>

            >>> frame.inspect()
            [#]  name      age  tenure  phone
            ====================================
            [0]  Fred       39      16  555-1234
            [1]  Susan      33       3  555-0202
            [2]  Thurston   65      26  555-4510
            [3]  Judy       44      14  555-2183

            >>> frame.add_columns(lambda row: row.age - 18, ('adult_years', ta.int32))
            <progress>

            >>> frame.inspect()
            [#]  name      age  tenure  phone     adult_years
            =================================================
            [0]  Fred       39      16  555-1234           21
            [1]  Susan      33       3  555-0202           15
            [2]  Thurston   65      26  555-4510           47
            [3]  Judy       44      14  555-2183           26


        Multiple columns can be added at the same time.  Let's add percentage of
        life and percentage of adult life in one call, which is more efficient.

        .. code::

            >>> frame.add_columns(lambda row: [row.tenure / float(row.age), row.tenure / float(row.adult_years)], [("of_age", ta.float32), ("of_adult", ta.float32)])
            <progress>
            >>> frame.inspect(round=2)
            [#]  name      age  tenure  phone     adult_years  of_age  of_adult
            ===================================================================
            [0]  Fred       39      16  555-1234           21    0.41      0.76
            [1]  Susan      33       3  555-0202           15    0.09      0.20
            [2]  Thurston   65      26  555-4510           47    0.40      0.55
            [3]  Judy       44      14  555-2183           26    0.32      0.54

        Note that the function returns a list, and therefore the schema also needs to be a list.

        It is not necessary to use lambda syntax, any function will do, as long as it takes a single row argument.  We
        can also call other local functions within.

        Let's add a column which shows the amount of person's name based on their adult tenure percentage.

            >>> def percentage_of_string(string, percentage):
            ...     '''returns a substring of the given string according to the given percentage'''
            ...     substring_len = int(percentage * len(string))
            ...     return string[:substring_len]

            >>> def add_name_by_adult_tenure(row):
            ...     return percentage_of_string(row.name, row.of_adult)

            >>> frame.add_columns(add_name_by_adult_tenure, ('tenured_name', unicode))
            <progress>

            >>> frame
            Frame <unnamed>
            row_count = 4
            schema = [name:unicode, age:int32, tenure:int32, phone:unicode, adult_years:int32, of_age:float32, of_adult:float32, tenured_name:unicode]
            status = ACTIVE  (last_read_date = -etc-)

            >>> frame.inspect(columns=['name', 'of_adult', 'tenured_name'], round=2)
            [#]  name      of_adult  tenured_name
            =====================================
            [0]  Fred          0.76  Fre
            [1]  Susan         0.20  S
            [2]  Thurston      0.55  Thur
            [3]  Judy          0.54  Ju


        **Optimization** - If we know up front which columns our row function will access, we
        can tell add_columns to speed up the execution by working on only the limited feature
        set rather than the entire row.

        Let's add a name based on tenure percentage of age.  We know we're only going to use
        columns 'name' and 'of_age'.

        .. code::

            >>> frame.add_columns(lambda row: percentage_of_string(row.name, row.of_age),
            ...                   ('tenured_name_age', unicode),
            ...                   columns_accessed=['name', 'of_age'])
            <progress>
            >>> frame.inspect(round=2)
            [#]  name      age  tenure  phone     adult_years  of_age  of_adult
            ===================================================================
            [0]  Fred       39      16  555-1234           21    0.41      0.76
            [1]  Susan      33       3  555-0202           15    0.09      0.20
            [2]  Thurston   65      26  555-4510           47    0.40      0.55
            [3]  Judy       44      14  555-2183           26    0.32      0.54
            <blankline>
            [#]  tenured_name  tenured_name_age
            ===================================
            [0]  Fre           F
            [1]  S
            [2]  Thur          Thu
            [3]  Ju            J

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

        .. code::

            >>> frame
            Frame <unnamed>
            row_count = 4
            schema = [name:unicode, age:int32, tenure:int32, phone:unicode, adult_years:int32, of_age:float32, of_adult:float32, tenured_name:unicode, tenured_name_age:unicode]
            status = ACTIVE  (last_read_date = -etc-)

            <skip>
            >>> frame2 = frame.copy()  # full copy of the frame
            <progress>
            </skip>

            >>> frame3 = frame.copy(['name', 'age'])  # copy only two columns
            <progress>
            >>> frame3
            Frame  <unnamed>
            row_count = 4
            schema = [name:unicode, age:int32]
            status = ACTIVE  (last_read_date = -etc-)

        .. code::

            >>> frame4 = frame.copy({'name': 'name', 'age': 'age', 'tenure': 'years'},
            ...                     where=lambda row: row.age > 40)
            <progress>
            >>> frame4.inspect()
            [#]  name      age  years
            =========================
            [0]  Thurston   65     26
            [1]  Judy       44     14

        """
        return self._backend.copy(self, columns, where, name)

    @api
    @arg('where', 'function', "|UDF| which evaluates a row to a boolean")
    @returns(int, "number of rows for which the where |UDF| evaluated to True.")
    def __count(self, where):
        """
        Counts the number of rows which meet given criteria.

        Examples
        --------

            <hide>
            >>> frame = _frame

            </hide>

            >>> frame.inspect()
            [#]  name      age  tenure  phone
            ====================================
            [0]  Fred       39      16  555-1234
            [1]  Susan      33       3  555-0202
            [2]  Thurston   65      26  555-4510
            [3]  Judy       44      14  555-2183
            >>> frame.count(lambda row: row.age > 35)
            <progress>
            3

        """

        return self._backend.get_row_count(self, where)

    @api
    @arg('n', int, 'The number of rows to download to this client from the frame (warning: do not overwhelm this client by downloading too much)')
    @arg('offset', int, 'The number of rows to skip before copying')
    @arg('columns', list, 'Column filter, the names of columns to be included (default is all columns)')
    @returns('pandas.DataFrame', 'A new pandas dataframe object containing the downloaded frame data' )
    def __download(self, n=100, offset=0, columns=None):
        """
        Download frame data from the server into client workspace as a pandas dataframe

        Similar to the 'take' function, but puts the data in a pandas dataframe.

        Examples
        --------

        .. code::

            >>> pandas_frame = frame.download(columns=['name', 'phone'])
            >>> pandas_frame
                   name     phone
            0      Fred  555-1234
            1     Susan  555-0202
            2  Thurston  555-4510
            3      Judy  555-2183

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
            try:
                pandas_df[[headers[i]]] = pandas_df[[headers[i]]].astype(dtype_str)
            except (TypeError, ValueError):
                if dtype_str.startswith("int"):
                    # DataFrame does not handle missing values in int columns. If we get this error, use the 'object' datatype instead.
                    print "WARNING - Encountered problem casting column %s to %s, possibly due to missing values (i.e. presence of None).  Continued by casting column %s as 'object'" % (headers[i], dtype_str, headers[i])
                    pandas_df[[headers[i]]] = pandas_df[[headers[i]]].astype("object")
                else:
                    raise
        return pandas_df

    @api
    @has_udf_arg
    @arg('predicate', 'function', "|UDF| which evaluates a row to a boolean; rows that answer True are dropped from the Frame")
    def __drop_rows(self, predicate):
        """
        Erase any row in the current frame which qualifies.

        Examples
        --------

        .. code::

            <hide>
            >>> frame = _frame.copy()
            <progress>

            </hide>

            >>> frame.inspect()
            [#]  name      age  tenure  phone
            ====================================
            [0]  Fred       39      16  555-1234
            [1]  Susan      33       3  555-0202
            [2]  Thurston   65      26  555-4510
            [3]  Judy       44      14  555-2183
            >>> frame.drop_rows(lambda row: row.name[-1] == 'n')  # drop people whose name ends in 'n'
            <progress>
            >>> frame.inspect()
            [#]  name  age  tenure  phone
            ================================
            [0]  Fred   39      16  555-1234
            [1]  Judy   44      14  555-2183

        More information on a |UDF| can be found at :doc:`/ds_apir`.
        """
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
            <hide>
            >>> frame = _frame.copy()
            <progress>

            </hide>

            >>> frame.inspect()
            [#]  name      age  tenure  phone
            ====================================
            [0]  Fred       39      16  555-1234
            [1]  Susan      33       3  555-0202
            [2]  Thurston   65      26  555-4510
            [3]  Judy       44      14  555-2183
            >>> frame.filter(lambda row: row.tenure >= 15)  # keep only people with 15 or more years tenure
            <progress>
            >>> frame.inspect()
            [#]  name      age  tenure  phone
            ====================================
            [0]  Fred       39      16  555-1234
            [1]  Thurston   65      26  555-4510

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
    @arg('group_by_columns', list, 'Column name or list of column names')
    @arg('*aggregation_arguments', dict, "Aggregation function based on entire row, and/or dictionaries (one or more) of { column name str : aggregation function(s) }.")
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
            *   The aggregation arguments also accepts the User Defined function(UDF). UDF acts on each row

        Examples
        --------
        For setup, we will use a Frame *my_frame* accessing a frame with a
        column *a*:

        .. code::

            <hide>

            >>> _group_by_rows = [[1, "alpha", 3.0, "small", 1, 3.0, 9],
            ...                   [1, "bravo", 5.0, "medium", 1, 4.0, 9],
            ...                   [1, "alpha", 5.0, "large", 1, 8.0, 8],
            ...                   [2, "bravo", 8.0, "large", 1, 5.0, 7],
            ...                   [2, "charlie", 12.0, "medium", 1, 6.0, 6],
            ...                   [2, "bravo", 7.0, "small", 1, 8.0, 5],
            ...                   [2, "bravo", 12.0, "large",  1, 6.0, 4]]
            >>> _group_by_schema = [("a",int), ("b",str), ("c",ta.float64), ("d",str), ("e", int), ("f", ta.float64), ("g", int)]
            >>> _group_by_frame = ta.Frame(ta.UploadRows(_group_by_rows, _group_by_schema))
            <progress>
            >>> frame = _group_by_frame

            </hide>

            >>> frame.inspect()
            [#]  a  b        c     d       e  f    g
            ========================================
            [0]  1  alpha     3.0  small   1  3.0  9
            [1]  1  bravo     5.0  medium  1  4.0  9
            [2]  1  alpha     5.0  large   1  8.0  8
            [3]  2  bravo     8.0  large   1  5.0  7
            [4]  2  charlie  12.0  medium  1  6.0  6
            [5]  2  bravo     7.0  small   1  8.0  5
            [6]  2  bravo    12.0  large   1  6.0  4

            Count the groups in column 'b'

            >>> b_count = frame.group_by('b', ta.agg.count)
            <progress>
            >>> b_count.inspect()
            [#]  b        count
            ===================
            [0]  alpha        2
            [1]  bravo        4
            [2]  charlie      1

            >>> avg1 = frame.group_by(['a', 'b'], {'c' : ta.agg.avg})
            <progress>
            >>> avg1.inspect()
            [#]  a  b        c_AVG
            ======================
            [0]  2  bravo      9.0
            [1]  1  alpha      4.0
            [2]  2  charlie   12.0
            [3]  1  bravo      5.0

            >>> mix_frame = frame.group_by('a', ta.agg.count, {'f': [ta.agg.avg, ta.agg.sum, ta.agg.min], 'g': ta.agg.max})
            <progress>
            >>> mix_frame.inspect()
            [#]  a  count  g_MAX  f_AVG  f_SUM  f_MIN
            =========================================
            [0]  1      3      9    5.0   15.0    3.0
            [1]  2      4      7   6.25   25.0    5.0

            >>> def custom_agg(acc, row):
            ...     acc.c_sum = acc.c_sum + row.c
            ...     acc.c_prod= acc.c_prod*row.c

            >>> sum_prod_frame = frame.group_by(['a', 'b'], ta.agg.udf(aggregator=custom_agg,output_schema=[('c_sum', ta.float64),('c_prod', ta.float64)],init_values=[0,1]))
            <progress>

            >>> sum_prod_frame.inspect()
            [#]  a  b        c_sum  c_prod
            ==============================
            [0]  2  bravo     27.0   672.0
            [1]  1  alpha      8.0    15.0
            [2]  2  charlie   12.0    12.0
            [3]  1  bravo      5.0     5.0

        For further examples, see :ref:`example_frame.group_by`.
        """
        return self._backend.group_by(self, group_by_columns, aggregation_arguments)

    @api
    @arg('*column_inputs', 'str | tuple(str, dict)', 'Comma-separated column names to summarize or tuple containing column name '
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

<skip>

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

</skip>
        """
        return self._backend.categorical_summary(self, column_inputs)

    @api
    @arg('n', int, 'The number of rows to print (warning: do not overwhelm this client by downloading too much)')
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
        To look at the first 4 rows of data in a frame:

        .. code::

        <skip>
            >>> frame.inspect(4)
            [#]  animal    name    age  weight
            ==================================
            [0]  human     George    8   542.5
            [1]  human     Ursula    6   495.0
            [2]  ape       Ape      41   400.0
            [3]  elephant  Shep      5  8630.0
        </skip>

        # For other examples, see :ref:`example_frame.inspect`.

        Note: if the frame data contains unicode characters, this method may raise a Unicode exception when
        running in an interactive REPL or otherwise which triggers the standard python repr().  To get around
        this problem, explicitly print the unicode of the returned object:

        .. code::

        <skip>
            >>> print unicode(frame.inspect())
        </skip>


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
    @arg('right', 'Frame', "Another frame to join with")
    @arg('left_on', list, "Names of the columns in the left frame used to match up the two frames.")
    @arg('right_on', list, "Names of the columns in the right frame used to match up the two frames. "
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
        This method take column(s) in the left frame and matches its values
        with column(s) in the right frame.
        Using the default 'how' option ['inner'] will only allow data in the
        resultant frame if both the left and right frames have the same value
        in the matching column(s).
        Using the 'left' 'how' option will allow any data in the resultant
        frame if it exists in the left frame, but will allow any data from the
        right frame if it has a value in its column(s) which matches the value in
        the left frame column(s).
        Using the 'right' option works similarly, except it keeps all the data
        from the right frame and only the data from the left frame when it
        matches.
        The 'outer' option provides a frame with data from both frames where
        the left and right frames did not have the same value in the matching
        column(s).

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

        <hide>
        >>> import trustedanalytics as ta
        >>> ta.connect()
        -etc-

        >>> codes = ta.Frame(ta.UploadRows([[1], [3], [1], [0], [2], [1], [5], [3]], [('numbers', ta.int32)]))
        -etc-

        >>> colors = ta.Frame(ta.UploadRows([[1, 'red'], [2, 'yellow'], [3, 'green'], [4, 'blue']], [('numbers', ta.int32), ('color', str)]))
        -etc-

        >>> country_code_rows = [[1, 354, "a"],[2, 91, "a"],[2, 100, "b"],[3, 47, "a"],[4, 968, "c"],[5, 50, "c"]]
        >>> country_code_schema = [("col_0", int),("col_1", int),("col_2",str)]
        -etc-

        >>> country_name_rows = [[1, "Iceland", "a"],[1, "Ice-land", "a"],[2, "India", "b"],[3, "Norway", "a"],[4, "Oman", "c"],[6, "Germany", "c"]]
        >>> country_names_schema = [("col_0", int),("col_1", str),("col_2",str)]
        -etc-

        >>> country_codes_frame = ta.Frame(ta.UploadRows(country_code_rows, country_code_schema))
        -etc-

        >>> country_names_frame= ta.Frame(ta.UploadRows(country_name_rows, country_names_schema))
        -etc-

        </hide>

        Consider two frames: codes and colors

        >>> codes.inspect()
        [#]  numbers
        ============
        [0]        1
        [1]        3
        [2]        1
        [3]        0
        [4]        2
        [5]        1
        [6]        5
        [7]        3


        >>> colors.inspect()
        [#]  numbers  color
        ====================
        [0]        1  red
        [1]        2  yellow
        [2]        3  green
        [3]        4  blue


        Join them on the 'numbers' column ('inner' join by default)

        >>> j = codes.join(colors, 'numbers')
        <progress>

        >>> j.inspect()
        [#]  numbers  color
        ====================
        [0]        1  red
        [1]        3  green
        [2]        1  red
        [3]        2  yellow
        [4]        1  red
        [5]        3  green

        (The join adds an extra column *_R which is the join column from the right frame; it may be disregarded)

        Try a 'left' join, which includes all the rows of the codes frame.

        >>> j_left = codes.join(colors, 'numbers', how='left')
        <progress>

        >>> j_left.inspect()
        [#]  numbers_L  color
        ======================
        [0]          1  red
        [1]          3  green
        [2]          1  red
        [3]          0  None
        [4]          2  yellow
        [5]          1  red
        [6]          5  None
        [7]          3  green


        And an outer join:

        >>> j_outer = codes.join(colors, 'numbers', how='outer')
        <progress>

        >>> j_outer.inspect()
        [#]  numbers_L  color
        ======================
        [0]          0  None
        [1]          1  red
        [2]          1  red
        [3]          1  red
        [4]          2  yellow
        [5]          3  green
        [6]          3  green
        [7]          4  blue
        [8]          5  None

        Consider two frames: country_codes_frame and country_names_frame

        >>> country_codes_frame.inspect()
        [#]  col_0  col_1  col_2
        ========================
        [0]      1    354  a
        [1]      2     91  a
        [2]      2    100  b
        [3]      3     47  a
        [4]      4    968  c
        [5]      5     50  c


        >>> country_names_frame.inspect()
        [#]  col_0  col_1     col_2
        ===========================
        [0]      1  Iceland   a
        [1]      1  Ice-land  a
        [2]      2  India     b
        [3]      3  Norway    a
        [4]      4  Oman      c
        [5]      6  Germany   c

        Join them on the 'col_0' and 'col_2' columns ('inner' join by default)

        >>> composite_join = country_codes_frame.join(country_names_frame, ['col_0', 'col_2'])
        <progress>

        >>> composite_join.inspect()
        [#]  col_0  col_1_L  col_2  col_1_R
        ====================================
        [0]      1      354  a      Iceland
        [1]      1      354  a      Ice-land
        [2]      2      100  b      India
        [3]      3       47  a      Norway
        [4]      4      968  c      Oman

        More examples can be found in the :ref:`user manual
        <example_frame.join>`.
        """
        return self._backend.join(self, right, left_on, right_on, how, name)

    @api
    @arg('columns', 'str | list of str | list of tuples', "Either a column name, a list of column names, or a "
         "list of tuples where each tuple is a name and an ascending bool value.")
    @arg('ascending', bool, "True for ascending, False for descending.")
    def __sort(self, columns, ascending=True):
        """
        Sort the data in a frame.

        Sort a frame by column values either ascending or descending.

        Examples
        --------

            <hide>
            >>> frame = ta.Frame(ta.UploadRows([[3, 'foxtrot'], [1, 'charlie'], [3, 'bravo'], [2, 'echo'], [4, 'delta'], [3, 'alpha']], [('col1', ta.int32), ('col2', unicode)]))
            -etc-

            </hide>

        Consider the frame
            >>> frame.inspect()
            [#]  col1  col2
            ==================
            [0]     3  foxtrot
            [1]     1  charlie
            [2]     3  bravo
            [3]     2  echo
            [4]     4  delta
            [5]     3  alpha

        Sort a single column:

        .. code::

            >>> frame.sort('col1')
            <progress>
            >>> frame.inspect()
            [#]  col1  col2
            ==================
            [0]     1  charlie
            [1]     2  echo
            [2]     3  foxtrot
            [3]     3  bravo
            [4]     3  alpha
            [5]     4  delta

        Sort a single column descending:

        .. code::

            >>> frame.sort('col2', False)
            <progress>
            >>> frame.inspect()
            [#]  col1  col2
            ==================
            [0]     3  foxtrot
            [1]     2  echo
            [2]     4  delta
            [3]     1  charlie
            [4]     3  bravo
            [5]     3  alpha

        Sort multiple columns:

        .. code::

            >>> frame.sort(['col1', 'col2'])
            <progress>
            >>> frame.inspect()
            [#]  col1  col2
            ==================
            [0]     1  charlie
            [1]     2  echo
            [2]     3  alpha
            [3]     3  bravo
            [4]     3  foxtrot
            [5]     4  delta


        Sort multiple columns descending:

        .. code::

            >>> frame.sort(['col1', 'col2'], False)
            <progress>
            >>> frame.inspect()
            [#]  col1  col2
            ==================
            [0]     4  delta
            [1]     3  foxtrot
            [2]     3  bravo
            [3]     3  alpha
            [4]     2  echo
            [5]     1  charlie

        Sort multiple columns: 'col1' decending and 'col2' ascending:

        .. code::

            >>> frame.sort([ ('col1', False), ('col2', True) ])
            <progress>
            >>> frame.inspect()
            [#]  col1  col2
            ==================
            [0]     4  delta
            [1]     3  alpha
            [2]     3  bravo
            [3]     3  foxtrot
            [4]     2  echo
            [5]     1  charlie

        """
        return self._backend.sort(self, columns, ascending)

    @api
    @arg('n', int, 'The number of rows to copy to this client from the frame (warning: do not overwhelm this client by downloading too much)')
    @arg('offset', int, "The number of rows to skip before starting to copy")
    @arg('columns', 'str | iterable of str', "If not None, only the given columns' data will be provided.  "
         "By default, all columns are included")
    @returns(list, "A list of lists, where each contained list is the data for one row.")
    def __take(self, n, offset=0, columns=None):
        """
        Get data subset.

        Take a subset of the currently active Frame.

        Examples
        --------
        .. code::

        <hide>
            >>> frame = _frame

        </hide>
            >>> frame.take(2)
            [[u'Fred', 39, 16, u'555-1234'], [u'Susan', 33, 3, u'555-0202']]

            >>> frame.take(2, offset=2)
            [[u'Thurston', 65, 26, u'555-4510'], [u'Judy', 44, 14, u'555-2183']]

        """
        result = self._backend.take(self, n, offset, columns)
        return result.data

@api
class Frame(_DocStubsFrame, _BaseFrame):
    """
    Large table of data.

    Acts as a proxy object to a frame of data on the server, with properties and functions to operate on that frame.
    """

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

        <skip>
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
        </skip>

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

        .. code::

            >>> animals = ta.Frame(ta.UploadRows([['dog', 'snoopy'],
            ...                                    ['cat', 'tom'],
            ...                                    ['bear', 'yogi'],
            ...                                    ['mouse', 'jerry']],
            ...                                    [('animal', str), ('name', str)]))
            <progress>
            >>> animals.append(ta.UploadRows([['donkey'],
            ...                                ['elephant'],
            ...                                ['ostrich']],
            ...                                [('animal', str)]))
            <progress>

            >>> animals.inspect()
            [#]  animal    name
            =====================
            [0]  dog       snoopy
            [1]  cat       tom
            [2]  bear      yogi
            [3]  mouse     jerry
            [4]  donkey    None
            [5]  elephant  None
            [6]  ostrich   None


        The data we added didn't have names, so None values were inserted for the new rows.
        """
        self._backend.append(self, data)


@api
class VertexFrame(_DocStubsVertexFrame, _BaseFrame):
    """A list of Vertices owned by a Graph.

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

    <skip>
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
    </skip>
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
    def __drop_rows(self, predicate):
        """
        Delete rows in this vertex frame that qualify.

        Parameters
        ----------
        predicate : |UDF|
            |UDF| or :term:`lambda` which takes a row argument and evaluates
            to a boolean value.

        Examples
        --------
        Create a frame, move the data to graph and then define a new VertexFrame and add data to it:

        .. code::

            >>> schema = [('viewer', str), ('profile', ta.int32), ('movie', str), ('rating', ta.int32)]
            >>> data = [['fred',0,'Croods',5],
            ...          ['fred',0,'Jurassic Park',5],
            ...          ['fred',0,'2001',2],
            ...          ['fred',0,'Ice Age',4],
            ...          ['wilma',0,'Jurassic Park',3],
            ...          ['wilma',0,'2001',5],
            ...          ['wilma',0,'Ice Age',4],
            ...          ['pebbles',1,'Croods',4],
            ...          ['pebbles',1,'Land Before Time',3],
            ...          ['pebbles',1,'Ice Age',5]]
            >>> frame = ta.Frame(ta.UploadRows(data, schema))
            <progress>

            >>> frame.inspect()
            [#]  viewer   profile  movie             rating
            ===============================================
            [0]  fred           0  Croods                 5
            [1]  fred           0  Jurassic Park          5
            [2]  fred           0  2001                   2
            [3]  fred           0  Ice Age                4
            [4]  wilma          0  Jurassic Park          3
            [5]  wilma          0  2001                   5
            [6]  wilma          0  Ice Age                4
            [7]  pebbles        1  Croods                 4
            [8]  pebbles        1  Land Before Time       3
            [9]  pebbles        1  Ice Age                5

            >>> graph = ta.Graph()

            >>> graph.define_vertex_type('viewer')
            <progress>

            >>> graph.define_vertex_type('film')
            <progress>

            >>> graph.define_edge_type('rating', 'viewer', 'film')
            <progress>

            >>> graph.vertices['viewer'].add_vertices(frame, 'viewer', ['profile'])
            <progress>

            >>> graph.vertices['viewer'].inspect()
            [#]  _vid  _label  viewer   profile
            ===================================
            [0]     1  viewer  fred           0
            [1]     8  viewer  pebbles        1
            [2]     5  viewer  wilma          0

            >>> graph.vertices['film'].add_vertices(frame, 'movie')
            <progress>

            >>> graph.vertices['film'].inspect()
            [#]  _vid  _label  movie
            ===================================
            [0]    19  film    Land Before Time
            [1]    14  film    Ice Age
            [2]    12  film    Jurassic Park
            [3]    11  film    Croods
            [4]    13  film    2001

            >>> graph.edges['rating'].add_edges(frame, 'viewer', 'movie', ['rating'])
            <progress>

            >>> graph.edges['rating'].inspect()
            [#]  _eid  _src_vid  _dest_vid  _label  rating
            ==============================================
            [0]    24         1         14  rating       4
            [1]    22         1         12  rating       5
            [2]    21         1         11  rating       5
            [3]    23         1         13  rating       2
            [4]    29         8         19  rating       3
            [5]    30         8         14  rating       5
            [6]    28         8         11  rating       4
            [7]    27         5         14  rating       4
            [8]    25         5         12  rating       3
            [9]    26         5         13  rating       5

        Call drop_rows() on the film VertexFrame to remove the row for the movie 'Croods' (vid = 11).
        Dangling edges (edges corresponding to 'Croods, vid = 11) are also removed.

        .. code::

            >>> graph.vertices['film'].drop_rows(lambda row: row.movie=='Croods')
            <progress>

            >>> graph.vertices['film'].inspect()
            [#]  _vid  _label  movie
            ===================================
            [0]    19  film    Land Before Time
            [1]    14  film    Ice Age
            [2]    12  film    Jurassic Park
            [3]    13  film    2001

            >>> graph.edges['rating'].inspect()
            [#]  _eid  _src_vid  _dest_vid  _label  rating
            ==============================================
            [0]    22         1         12  rating       5
            [1]    25         5         12  rating       3
            [2]    23         1         13  rating       2
            [3]    26         5         13  rating       5
            [4]    24         1         14  rating       4
            [5]    30         8         14  rating       5
            [6]    27         5         14  rating       4
            [7]    29         8         19  rating       3

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
    def __init__(self, graph=None, label=None, src_vertex_label=None, dest_vertex_label=None, directed=None, _info=None):
        """

    Examples
    --------
    Given a data file */movie.csv*, create a frame to match this data and move
    the data to the frame.
    Create an empty graph and define some vertex and edge types.

    <skip>
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

    </skip>
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
