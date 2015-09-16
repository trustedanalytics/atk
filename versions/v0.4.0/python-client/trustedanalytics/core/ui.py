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

import trustedanalytics as ta

spaces_between_cols = 2  # consts
ellipses = '...'


class RowsInspection(object):
    """
    class used specifically for inspect, where the __repr__ is the main use case
    """

    def __init__(self, rows, schema, offset, wrap=None, truncate=None, round=None, width=80, margin=None):
        if isinstance(wrap, basestring):
            if wrap == 'stripes':
                self._repr = self._stripes
            else:
                raise ValueError("argument wrap must be an integer or the string 'stripes'.  Received '%s'" % wrap)
        elif isinstance(wrap, int) and wrap >= 0:
            self.wrap = min(wrap, len(rows)) or len(rows)
            self._repr = self._wrap
        else:
            raise TypeError("argument wrap must be an integer or the string 'stripes'.  Received '%s'" % wrap)

        if not rows:
            self._repr = self._empty

        self.rows = rows
        self.schema = schema
        self.offset = offset
        self.truncate = get_validated_positive_int("truncate", truncate)
        self.round = get_validated_positive_int("round_floats", round)
        self.width = get_validated_positive_int("width", width)
        self.margin = get_validated_positive_int("margin", margin)
        self.value_formatters = [self._get_value_formatter(data_type) for name, data_type in schema]

    def __repr__(self):
        return self._repr()

    def _empty(self):
        return "(empty)"

    def _wrap(self):
        """print rows in a 'clumps' style"""
        row_index_str_format = '[%s]' + ' ' * spaces_between_cols

        def _get_row_index_str(index):
            return row_index_str_format % index

        row_count = len(self.rows)
        row_clump_count = _get_row_clump_count(row_count, self.wrap)
        header_sizes = _get_schema_name_sizes(self.schema)
        column_spacer = ' ' * spaces_between_cols
        lines_list = []
        extra_tuples = []

        for row_clump_index in xrange(row_clump_count):
            start_row_index = row_clump_index * self.wrap
            stop_row_index = start_row_index + self.wrap
            if stop_row_index > row_count:
                stop_row_index = row_count
            row_index_header = _get_row_index_str('#' * len(str(self.offset+stop_row_index-1)))
            margin = len(row_index_header)
            col_sizes = _get_col_sizes(self.rows, start_row_index, self.wrap, header_sizes, self.value_formatters)
            col_index = 0
            while col_index < len(self.schema):
                num_cols = _get_num_cols(self.schema, self.width, col_index, col_sizes, margin)
                if num_cols == 0:
                    raise RuntimeError("Internal error, num_cols == 0")  # sanity check on algo
                header_line = row_index_header + column_spacer.join([pad_left(name, min(self.width - margin, col_sizes[col_index+i])) for i, (name, data_type) in enumerate(self.schema[col_index:col_index+num_cols])])
                thick_line = "=" * len(header_line)
                lines_list.extend(["", header_line, thick_line])
                for row_index in xrange(start_row_index, stop_row_index):
                    new_line = pad_right(_get_row_index_str(self.offset+row_index), margin) + column_spacer.join([self._get_wrap_entry(data, col_sizes[col_index+i], self.value_formatters[col_index+i], i, extra_tuples) for i, data in enumerate(self.rows[row_index][col_index:col_index+num_cols])])
                    lines_list.append(new_line.rstrip())
                    if extra_tuples:
                        lines_list.extend(_get_lines_from_extra_tuples(extra_tuples, col_sizes[col_index:col_index+num_cols], margin))
                col_index += num_cols

            lines_list.append('')  # extra line for new clump

        return "\n".join(lines_list)

    def _stripes(self):
        """print rows as stripes style"""
        max_margin = 0
        for name, data_type in self.schema:
            length = len(name) + 1 # to account for the '='
            if length > max_margin:
                max_margin = length
        if not self.margin or max_margin < self.margin:
            self.margin = max_margin
        lines_list = []
        for row_index in xrange(len(self.rows)):
            lines_list.append(self._get_stripe_header(self.offset+row_index))
            lines_list.extend([self._get_stripe_entry(i, name, value)
                               for i, (name, value) in enumerate(zip(map(lambda x: x[0], self.schema),
                                                                     self.rows[row_index]))])
        return "\n".join(lines_list)

    def _get_stripe_header(self, index):
        row_number = "[%s]" % index
        return row_number + "-" * (self.margin - len(row_number))

    def _get_stripe_entry(self, i, name, value):
        return "%s=%s" % (pad_right(name, self.margin - 1), self.value_formatters[i](value))

    def _get_value_formatter(self, data_type):
        if self.round and is_type_float(data_type):
            return self.get_rounder(data_type)
        if self.truncate and is_type_unicode(data_type):
            return self.get_truncater()
        return identity

    @staticmethod
    def _get_wrap_entry(data, size, formatter, relative_column_index, extra_tuples):
        entry = unicode(formatter(data))
        if isinstance(data, basestring):
            lines = entry.splitlines()
            if len(lines) > 1:
                entry = lines[0]  # take the first line now, and save the rest in an 'extra' tuple
                extra_tuples.append((relative_column_index, lines[1:]))
            return pad_right(entry, size)
        else:
            return pad_left(entry, size)

    def get_truncater(self):
        target_len = self.truncate

        def truncate_string(s):
            return truncate(s, target_len)
        return truncate_string

    def get_rounder(self, float_type):
        num_digits = self.round
        if isinstance(float_type, ta.vector):
            def vector_rounder(value):
                return round_vector(value, num_digits)
            return vector_rounder

        def rounder(value):
            return round_float(value, float_type, num_digits)
        return rounder


def is_type_float(t):
    tpe = ta.valid_data_types.get_from_type(t)
    return tpe is ta.float32 or tpe is ta.float64 or isinstance(t, ta.vector)


def is_type_unicode(t):
    return ta.valid_data_types.get_from_type(t) is unicode


def get_validated_positive_int(arg_name, arg_value):
    if arg_value is not None:
        if not isinstance(arg_value, int):
            raise TypeError('%s argument must be an integer, got %s' % (arg_name, type(arg_value)))
        if arg_value <= 0:
            raise ValueError('%s argument must be an integer > 0, got %s' % (arg_name, arg_value))
    return arg_value


def pad_left(s, target_len):
    """pads string s on the left such that is has at least length target_len"""
    return ' ' * (target_len - len(s)) + s


def pad_right(s, target_len):
    """pads string s on the right such that is has at least length target_len"""
    return s + ' ' * (target_len - len(s))


def truncate(s, target_len):
    """truncates string to the target_len"""
    if target_len < len(ellipses):
        raise ValueError("Bad truncate length %s.  "
                         "Must be set to at least %s to allow for a '%s'." % (target_len, len(ellipses), ellipses))
    if len(s) <= target_len:
        return s
    return s[:target_len - len(ellipses)] + ellipses


def round_float(f, float_type, num_digits):
    """provides a rounded, formatted string for the given number of decimal places"""
    value = float_type(f)
    max_len = len(str(value).split('.')[1])
    padding = '0' * (num_digits - max_len)
    template = "%%.%df%s" % (min(num_digits, max_len) or 1, padding)
    return template % float_type.round(float_type(f), num_digits)


def round_vector(v, num_digits):
    """provides a rounded, formatted string to represent the vector"""
    return "[%s]" % ", ".join([round_float(f, ta.float64, num_digits) for f in v])


def identity(value):
    return value


def _get_col_sizes(rows, row_index, row_count, header_sizes, formatters):
    sizes = list(header_sizes)
    for r in xrange(row_index, row_index+row_count):
        if r < len(rows):
            row = rows[r]
            for c in xrange(len(sizes)):
                entry = unicode(formatters[c](row[c]))
                lines = entry.splitlines()
                max = 0
                for line in lines:
                    length = len(line)
                    if length > max:
                        max = length
                if max > sizes[c]:
                    sizes[c] = max
    return sizes


def _get_num_cols(schema, width, start_col_index, col_sizes, margin):
    """goes through the col_sizes starting at the given index and finds
       how many columns can be included on a line"""
    num_cols = 0
    line_length = margin - spaces_between_cols
    while line_length < width and start_col_index + num_cols < len(schema):
        candidate = col_sizes[start_col_index + num_cols] + spaces_between_cols
        if (line_length + candidate) > width:
            if num_cols == 0:
                num_cols = 1
            break
        num_cols += 1
        line_length += candidate

    return num_cols


def _get_schema_name_sizes(schema):
    return [len("%s" % name) for name, data_type in schema]


def _get_row_clump_count(row_count, wrap):
    return row_count / wrap + (1 if row_count % wrap else 0)


def _get_lines_from_extra_tuples(tuples, col_sizes, margin):
    # (for wrap formatting)
    # tuples is a list of tuples of the form (relative column index, [lines])
    # col_sizes is an array of the col_sizes for the 'current' clump (hence
    #   the 'relative column index' in the tuples list --these indices match
    #
    new_lines = []  # list of new, full-fledged extra lines that come from the tuples

    def there_are_tuples_in(x):
        return bool(len(x))

    while there_are_tuples_in(tuples):
        tuple_index = 0
        new_line_columns = [' ' * margin]
        for size_index in xrange(len(col_sizes)):
            if tuple_index < len(tuples) and size_index == tuples[tuple_index][0]:
                index, lines = tuples[tuple_index]  # the 'tuple'
                entry = lines.pop(0)
                new_line_columns.append(pad_right(entry, col_sizes[size_index]))
                if not len(lines):
                    del tuples[tuple_index]  # remove empty tuple, which also naturally moves index to the next tuple
                else:
                    tuple_index += 1  # move on to the next tuple
            else:
                new_line_columns.append(' ' * (col_sizes[size_index] + spaces_between_cols))

        new_lines.append(''.join(new_line_columns).rstrip())

    return new_lines
