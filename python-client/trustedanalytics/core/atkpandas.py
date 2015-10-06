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

from trustedanalytics.core.atktypes import valid_data_types, datetime


def atk_dtype_to_pandas_str(dtype):
    """maps ATK schema types to types understood by pandas, returns string"""
    if dtype is not datetime and valid_data_types.is_primitive_type(dtype):
        return valid_data_types.to_string(dtype)
    return "object"


class Pandas(object):
    """
    Defines a pandas data source
    """

    # TODO - Review docstring
    annotation = "pandas_frame"

    def __init__(self, pandas_frame, schema, row_index=True):
        """
        Defines a pandas data source

        Parameters
        ----------
        pandas_frame : a pandas dataframe object
        schema : list of tuples of the form (string, type)
            schema description of the fields for a given line.
            It is a list of tuples which describe each field, (field name, field type),
            where the field name is a string, and file is a supported type,
            (See data_types from the atktypes module).
            Unicode characters should not be used in the column name.
        row_index : boolean (optional)
            indicates if the row_index is present in the pandas dataframe and needs to be ignored when looking at the
            data values.
            Default value is True.

        Returns
        -------
        class
            An object which holds both the pandas dataframe and schema associated with it.

        Examples
        --------
        For this example, we are going to create a 0-5 ratings system with corresponding descriptions.
        It consists of two columns, rating number and rating description.
        The columns have the data types, int32 and string.

        First import trustedanalytics and pandas::

            import trustedanalytics as ta
            import pandas

        Connect::

            ta.connect()

        Create data::

            ratings_data = [[0, "invalid"], [1, "Very Poor"], [2, "Poor"], [3, "Average"], [4, "Good"], [5, "Very Good"]]
            df = pandas.DataFrame(ratings_data, columns=['rating_id', 'rating_text'])

        At this point create a schema that defines the data::

            schema = [('rating_id', ta.int32), ('rating_text', unicode)]

        Now build a PandasFrame object with this schema::

            ratings= ta.Frame(ta.Pandas(df, schema))

        To check the result:: 
            ratings.inspect()




        """
        import pandas
        if not isinstance(pandas_frame, pandas.DataFrame):
            raise ValueError("pandas_frame must be a pandas Data Frame")
        if not schema:
            raise ValueError("schema must be non-empty list of tuples")
        self.pandas_frame = pandas_frame
        self.schema = list(schema)
        self._validate()
        self.row_index = row_index

    def __repr__(self):
        return repr(self.schema)

    def _schema_to_json(self):
        return [(field[0], valid_data_types.to_string(field[1]))
                for field in self.schema]

    @property
    def field_names(self):
        """
        Schema field names.

        List of field names from the schema stored in the trustedanalytics pandas dataframe object

        Returns
        -------
        list of string
            Field names

        Examples
        --------
        For this example, we are going to use a pandas dataframe object *your_pandas* .
        It will have two columns *col1* and *col2* with types of *int32* and *float32* respectively::

            my_pandas = ta.PandasFrame(your_pandas, schema=[("col1", ta.int32), ("col2", ta.float32)])
            print(my_pandas.field_names())

        The output would be::

            ["col1", "col2"]

        """
        # TODO - Review docstring
        return [x[0] for x in self.schema]

    @property
    def field_types(self):
        """
        Schema field types

        List of field types from the schema stored in the trustedanalytics pandas dataframe object.

        Returns
        -------
        list of types
            Field types

        Examples
        --------
        For this example, we are going to use a pandas dataframe object *your_pandas* .
        It will have two columns *col1* and *col2* with types of *int32* and *float32* respectively::

            my_pandas = ta.PandasFrame(your_pandas, schema=[("col1", ta.int32), ("col2", ta.float32)])
            print(my_csv.field_types())

        The output would be::

            [numpy.int32, numpy.float32]

        """
        # TODO - Review docstring
        return [x[1] for x in self.schema]

    def _validate(self):
        validated_schema = []
        for field in self.schema:
            name = field[0]
            if not isinstance(name, basestring):
                raise ValueError("First item in schema tuple must be a string")
            try:
                data_type = valid_data_types.get_from_type(field[1])
            except ValueError:
                raise ValueError("Second item in schema tuple must be a supported type: " + str(valid_data_types))
            else:
                validated_schema.append((name, data_type))
        self.schema = validated_schema
