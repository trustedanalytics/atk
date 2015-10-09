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
Library for creating pieces of rst text for the Python API, based on metaprog
"""

import re
from collections import OrderedDict

from trustedanalytics.meta.command import Doc
from trustedanalytics.meta.metaprog import get_installation, get_intermediate_class
from trustedanalytics.meta.names import is_name_private, indent, get_type_name
from trustedanalytics.meta.reflect import get_args_text_from_function


def get_command_def_rst(command_def):
    """return rst entry for a function or attribute described by the command def"""
    one_line = command_def.doc.one_line
    if command_def.maturity:
        one_line = get_maturity_rst(command_def.maturity) + "\n" + one_line
    extended = command_def.doc.extended
    display_name = command_def.rst_info.display_name if hasattr(command_def, "rst_info") else command_def.name
    if command_def.is_property:
        args_text = ""
        directive = "attribute"
    else:
        args_text = "(%s)" % command_def.get_function_args_text()
        directive = "function"

    rst = """
.. {directive}:: {name}{args_text}

{one_line}

""".format(directive=directive, name=display_name, args_text=args_text, one_line=indent(one_line))


    if command_def.parameters:
        rst += indent(_get_parameters_rst(command_def))
    if command_def.return_info:
        rst += indent(_get_returns_rst(command_def.return_info))

    rst += indent(extended)
    if command_def.doc.examples:
        rst += indent(_get_examples_rst(command_def.doc.examples))
    return rst


def _get_parameters_rst(command_def):
    return """
:Parameters:
%s
""" % indent("\n".join([_get_parameter_rst(p) for p in command_def.parameters if not p.use_self]))


def _get_parameter_rst(p):
    data_type = get_type_name(p.data_type)
    if p.optional:
        data_type += " (default=%s)" % (p.default if p.default is not None else "None")
    return """
**{name}** : {data_type}

..

{description}

""".format(name=p.name, data_type=data_type, description=indent(p.doc))


def _get_returns_rst(return_info):
    return """

:Returns:

    : {data_type}

    ..

{description}
""".format(data_type=get_type_name(return_info.data_type), description=indent(return_info.doc, 8))


def _get_examples_rst(examples):
    if not examples:
        return ''
    return """

%s
""" % examples  # todo --all the example .rst files start with an "Examples" header.  It would be better to have that header here.


def get_maturity_rst(maturity):
    """rst markup for the maturity tags"""
    if maturity:
        return '|' + maturity.upper() + '|'
    else:
        return ''


def get_class_rst(cls):
    """
    Gets rst text to describe a class

    Only lists members in summary tables with hyperlinks to other rst docs, with the exception of __init__
    which is described in class rst text, below the summary tables.
    """
    sorted_members = get_member_rst_list(cls)  # sort defs for both summary table and attribute tables
    toctree = indent("\n".join([m.rst_name for m in sorted_members]))

    starter = """

.. toctree::
    :hidden:

{toctree}

.. class:: {name}

{rst_doc}""".format(toctree=toctree, name=get_type_name(cls), rst_doc=indent(cls.__doc__))

    lines = [starter]
    lines.append(indent(get_attribute_summary_table(sorted_members), 4))
    lines.append(indent(get_method_summary_table(sorted_members), 4))

    installation = get_installation(cls)
    init_commands = [c for c in installation.commands if c.is_constructor]
    if init_commands:
        lines.append("\n.. _%s:\n" % get_cls_init_rst_label(cls))  # add rst label for internal reference
        lines.append(get_command_def_rst(init_commands[0]))

    return "\n".join(lines)


def get_member_rst_list(cls, intermediate_prefix=None):
    """Gets list of RstInfo objects for all the members of the given class"""
    prefix = intermediate_prefix if intermediate_prefix else ''
    # create a ordered dict of names -> whether the name is inherited or not, based on do dir() vs. __dict__ comparison
    members = OrderedDict([(name, name not in cls.__dict__) for name in dir(cls) if not is_name_private(name)])  # True means we assume all are inherited

    member_rst_list = [RstInfo(cls, getattr(cls, name), prefix + name, is_inherited) for name, is_inherited in members.items()]

    # add members accessed through intermediate properties, for example, graph.sampling.vertex_sample will appear as 'sampling.vertex_sample'
    properties = [(name, get_intermediate_class(name, cls)) for name in members.keys() if isinstance(getattr(cls, name), property)]
    for property_name, intermediate_cls in properties:
        if intermediate_cls:
            p = prefix + property_name + '.'
            member_rst_list.extend(get_member_rst_list(intermediate_cls, intermediate_prefix=p))  # recursion

    return sorted(member_rst_list, key=lambda m: m.display_name)


def get_cls_init_rst_label(cls):
    """Gets the rst label (by our convention) for the init method so sphinx can create appropriate hyperlink"""
    return "%s__init__" % get_type_name(cls)


class RstInfo(object):
    """Represents a member to be rendered with .rst"""
    def __init__(self, cls, member, display_name, is_inherited):
        self.display_name = display_name
        self.is_inherited = is_inherited
        self.in_method_table = hasattr(member,  '__call__') and not self.is_private
        self.args_text = '' if not self.in_method_table else "(%s)" % get_args_text_from_function(member)
        if isinstance(member, property):
            member = member.fget
        command_def = getattr(member, "command") if hasattr(member, "command") else None

        if self.display_name[-8:] == "__init__":
            if len(self.display_name) > 8:  # mask (make private) the __init__ methods for properties (intermediates)
                self.display_name = "__private__init__"
            self.summary_rst_name = ":ref:`__init__ <%s>`\ " % get_cls_init_rst_label(cls)
            self.rst_name = command_def.name if command_def else ""
        else:
            self.summary_rst_name = ":doc:`%s <%s>`\ " % (self.display_name, self.display_name.replace('.', '/'))
            self.rst_name = self.display_name.replace('.', '/')

        intermediate_class = get_intermediate_class(display_name, cls)
        if intermediate_class:
            self.display_name = "__private_" + self.display_name

        if command_def:
            command_def.rst_info = self
            self.maturity = command_def.maturity
        else:
            self.maturity = None
            #self.name = self.display_name
        self.doc = command_def.doc if command_def else self.doc_to_rst(member.__doc__)

    @property
    def is_private(self):
        return is_name_private(self.display_name)

    @property
    def in_attribute_table(self):
        return not self.in_method_table and not self.is_private

    def get_summary_table_entry(self):
        summary = self.doc.one_line
        if self.maturity:
            summary = get_maturity_rst(self.maturity) + " " + summary

        if self.in_method_table:
            signature = self.get_signature_for_summary()
        else:
            signature = ''

        first_half = self.summary_rst_name + signature
        spaces = " " * (index_of_summary_start - len(first_half))
        return first_half + spaces + summary

    def get_signature_for_summary(self):
        if not self.args_text:
            sig = ''
        else:
            sig = mangle_signature(self.args_text, max_chars=get_signature_max(self.summary_rst_name))
            sig = sig.replace('*', r'\*')
        return sig

    @staticmethod
    def doc_to_rst(doc):
        """Create a Doc object if not already a Doc"""
        return doc if isinstance(doc, Doc) else Doc.get_from_str(doc)


###############################################################################
# Summary Tables

# parameters for summary table, signature mangling computation...
first_column_header_char_count = 100
second_column_header_char_count = 100
number_of_spaces_between_columns = 2
index_of_summary_start = first_column_header_char_count + number_of_spaces_between_columns
table_line = "=" * first_column_header_char_count + " " * number_of_spaces_between_columns + "=" * second_column_header_char_count


def get_signature_max(rst_name):
    return first_column_header_char_count - len(rst_name)


def get_method_summary_table(sorted_members):
    lines = ["\n.. rubric:: Methods", "", table_line]
    for member in sorted_members:
        if member.in_method_table:
            lines.append(member.get_summary_table_entry())
    lines.append(table_line)
    return "\n".join(lines)


def get_attribute_summary_table(sorted_members):
    lines = ["\n.. rubric:: Attributes", "", table_line]
    for member in sorted_members:
        if member.in_attribute_table:
            lines.append(member.get_summary_table_entry())
    lines.append(table_line)
    return "\n".join(lines)


#########################################################################################
# rest of the file taken from sphinx/ext/autosummary/__init__.py, for signature mangling


max_item_chars = 50


# sig should be args only --i.e. what's inside the parentheses
def mangle_signature(sig, max_chars=30):
    """Reformat a function signature to a more compact form."""
    s = re.sub(r"^\((.*)\)$", r"\1", sig).strip()

    # Strip strings (which can contain things that confuse the code below)
    s = re.sub(r"\\\\", "", s)
    s = re.sub(r"\\'", "", s)
    s = re.sub(r"'[^']*'", "", s)

    # Parse the signature to arguments + options
    args = []
    opts = []

    opt_re = re.compile(r"^(.*, |)([a-zA-Z0-9_*]+)=")
    while s:
        m = opt_re.search(s)
        if not m:
            # The rest are arguments
            args = s.split(', ')
            break

        opts.insert(0, m.group(2))
        s = m.group(1)[:-2]

    # Produce a more compact signature
    sig = limited_join(", ", args, max_chars=max_chars-2)
    if opts:
        if not sig:
            sig = "[%s]" % limited_join(", ", opts, max_chars=max_chars-4)
        elif len(sig) < max_chars - 4 - 2 - 3:
            sig += "[, %s]" % limited_join(", ", opts,
                                           max_chars=max_chars-len(sig)-4-2)

    return u"(%s)" % sig


def limited_join(sep, items, max_chars=30, overflow_marker="..."):
    """Join a number of strings to one, limiting the length to *max_chars*.

    If the string overflows this limit, replace the last fitting item by
    *overflow_marker*.

    Returns: joined_string
    """
    full_str = sep.join(items)
    if len(full_str) < max_chars:
        return full_str

    n_chars = 0
    n_items = 0
    for j, item in enumerate(items):
        n_chars += len(item) + len(sep)
        if n_chars < max_chars - len(overflow_marker):
            n_items += 1
        else:
            break

    return sep.join(list(items[:n_items]) + [overflow_marker])
