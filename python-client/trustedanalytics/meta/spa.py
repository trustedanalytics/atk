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
Static Program Analysis (SPA) docstring lib
"""

import re
from trustedanalytics.meta.names import indent, get_type_name


def get_spa_docstring(command_def, override_rtype=None):
    """return text for a docstring needed for SPA, uses classic rst format"""
    try:
        doc_str = str(command_def.doc)
    except:
        print "Problem with command_def.doc for command %s" % command_def.full_name
        raise

    params = command_def.parameters
    if params:
        params_text = "\n".join([get_parameter_text(p) for p in params if not p.use_self])
        doc_str += ("\n\n" + params_text)
    if command_def.return_info:
        doc_str += ("\n\n" + get_returns_text(command_def.return_info, override_rtype))

    doc_str = re.sub(r':term:`(.+)`', r'\1', doc_str)  # clear out some sphinx markup (could cover more....)
    return indent(doc_str)


def get_parameter_text(p):
    description = indent(p.doc)[4:]  # indents, but grabs the first line's space back
    if p.optional:
        description = "(default=%s)  " % (p.default if p.default is not None else "None") + description

    return ":param {name}: {description}\n:type {name}: {data_type}".format(name=p.name,
                                                                            description=description,
                                                                            data_type=get_type_name(p.data_type))


def get_returns_text(return_info, override_rtype):
    description = indent(return_info.doc)[4:]  # indents, but grabs the first line's space back
    return ":returns: {description}\n:rtype: {data_type}".format(description=description,
                                                                 data_type=get_type_name(override_rtype
                                                                                         or return_info.data_type))
