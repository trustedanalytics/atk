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
Formatting for numpydoc docstrings
"""
from trustedanalytics import valid_data_types


def get_numpy_doc(command_def, one_line, extended_summary=''):
    return repr(CommandNumpyDoc(command_def, one_line, extended_summary))


class CommandNumpyDoc(object):
    """
    Creates a doc string for the given CommandDefinition according to the numpydoc reStructuredText markup

    (instantiate, grab repr, discard)


    Parameters
    ----------
    command_def : CommandDefinition
        command definition
    one_line : str
        simple, one-line summary
    extended_summary : str (optional)
        more involved summary text
    """
    def __init__(self, command_def, one_line, extended_summary=''):
        self.command_def = command_def
        self.doc = self._create_doc(one_line, extended_summary, command_def.maturity)

    def __repr__(self):
        return self.doc

    # numpydoc sections for the function:
    #   short_summary
    #   extended_summary
    #   parameters
    #   returns
    #   raises
    #   notes
    #   examples
    #   version

    @staticmethod
    def _get_header(section_name):
        return "\n    %s\n    %s" % (section_name, '-' * len(section_name))

    def _format_summary(self, summary):
        summary = summary.rstrip()
        # make sure it ends with a period
        if summary and summary[-1] != '.':
            summary = summary + "."
        return summary

    def _format_parameters(self):
        items = [self._get_header("Parameters")]
        template = """    %s : %s %s\n%s"""  # $name : $data_type $optional\n$description
        for p in self.command_def.parameters:
            if not p.use_self:
                items.append(template % (p.name,
                                         self._get_data_type_str(p.data_type),
                                         '(optional)' if p.optional else '',
                                         self._indent(p.doc, spaces=8)))
        return "\n".join(items)

    def _format_returns(self):
        template = """%s\n    %s\n%s"""
        return template % (self._get_header("Returns"),
                           self._get_data_type_str(self.command_def.return_type.data_type),
                           self._indent(self.command_def.return_type.doc, spaces=8))

    @staticmethod
    def _indent(text, spaces=4):
        """splits text into lines and indents it"""
        indentation = ' ' * spaces + '%s'
        return "\n".join([indentation % s.strip() for s in text.split("\n")]) if text else ''

    @staticmethod
    def _get_data_type_str(data_type):
        """returns friendly string form of the data_type"""
        try:
            return valid_data_types.to_string(data_type)
        except:
            try:
                return data_type.__name__
            except:
                return str(data_type)

    def _create_doc(self, one_line, extended_summary, maturity, ignore_parameters=True):
        """Assembles the doc sections and creates a full doc string for this function"""
        sections = []
        if one_line:
            sections.append(self._format_summary(one_line))
        if maturity:
            sections.append("    |%s|" % maturity.upper())
        if extended_summary:
            sections.append(extended_summary)
        # TODO - get the parameter documentation in place and remove ignore_parameters arg
        if not ignore_parameters:
            if self.command_def.parameters:
                sections.append(self._format_parameters())
            if self.command_def.return_type:
                sections.append(self._format_returns())
        # TODO - raises
        # TODO - notes
        # TODO - examples
        return "\n".join(sections)
