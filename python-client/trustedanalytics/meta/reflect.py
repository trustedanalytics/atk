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
Python reflection with inspect
"""
import inspect
from trustedanalytics.meta.names import is_name_private, default_value_to_str


def get_args_text_from_function(function, ignore_self=False, ignore_private_args=False):
    args, kwargs, varargs, varkwargs = get_args_spec_from_function(function, ignore_self, ignore_private_args)
    args_text = ", ".join(args + ["%s=%s" % (name, default_value_to_str(value)) for name, value in kwargs])
    if varargs:
        args_text += (', *' + varargs)
    if varkwargs:
        args_text += (', **' + varkwargs)
    return args_text


def get_args_spec_from_function(function, ignore_self=False, ignore_private_args=False):
    args, varargs, varkwargs, defaults = inspect.getargspec(function)
    if ignore_self:
        args = [a for a in args if a != 'self']

    num_defaults = len(defaults) if defaults else 0
    if num_defaults:
        kwargs = zip(args[-num_defaults:], defaults)
        args = args[:-num_defaults]
    else:
        kwargs = []

    if ignore_private_args:
        args = [name for name in args if not is_name_private(name)]
        kwargs = [(name, value) for name, value in kwargs if not is_name_private(name)]
    return args, kwargs, varargs, varkwargs
