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
Decorator for functions which take UDFs
"""

import re
import sys
from decorator import decorator

def _has_udf_arg(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except:
        exc_info = sys.exc_info()
        e = exc_info[1]
        message = str(e)
        lines = message.split("\n")

        eligible_lines = []

        # match the server side stack trace from running python user function and remove it
        regex = re.compile(".*java:[0-9]+.*|.*scala:[0-9]+.*|Driver stacktrace.*")
        for line in lines:
            if regex.search(line) is None:
                eligible_lines.append(line)

        message = "\n".join(eligible_lines)
        e.args = (message,)
        raise e

def has_udf_arg(function):
    return decorator(_has_udf_arg, function)
