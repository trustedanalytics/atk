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
API context
"""

import logging
logger = logging.getLogger('meta')

from decorator import decorator

from trustedanalytics.core.errorhandle import IaError
from trustedanalytics.core.loggers import log_api_call
from trustedanalytics.core.api import api_status


class _ApiCallStack(object):
    """Tracks call depth in the call from the API"""

    def __init__(self):
        self._depth = 0

    @property
    def is_empty(self):
        return self._depth == 0

    def inc(self):
        self._depth += 1

    def dec(self):
        self._depth -= 1
        if self._depth < 0:
            self._depth = 0
            raise RuntimeError("Internal error: API call stack tracking went below zero")

_api_call_stack = _ApiCallStack()


class ApiCallLoggingContext(object):

    def __init__(self, call_stack, call_logger, call_depth, function, *args, **kwargs):
        self.logger = None
        self.call_stack = call_stack
        if self.call_stack.is_empty:
            self.logger = call_logger
            self.depth = call_depth
            self.function = function
            self.args = args
            self.kwargs = kwargs

    def __enter__(self):
        if self.call_stack.is_empty:
            log_api_call(self.depth, self.function, *self.args, **self.kwargs)
        self.call_stack.inc()

    def __exit__(self, exception_type, exception_value, traceback):
        self.call_stack.dec()
        if exception_value:
            error = IaError(self.logger)


            raise error  # see trustedanalytics.errors.last for details



def api_context(logger, depth, function, *args, **kwargs):
    global _api_call_stack
    return ApiCallLoggingContext(_api_call_stack, logger, depth, function, *args, **kwargs)


def get_api_context_decorator(execution_logger, call_depth=4):
    """Parameterized decorator which will wrap function for execution in the api context"""

    depth = call_depth
    exec_logger = execution_logger
    verify_api_installed = api_status.verify_installed

    # Note: extra whitespace lines in the code below is intentional for pretty-printing when error occurs
    def execute_in_api_context(function, *args, **kwargs):
        with api_context(logger, depth, function, *args, **kwargs):
            try:
                verify_api_installed()
                return function(*args, **kwargs)
            except:
                error = IaError(exec_logger)



                raise error  # see trustedanalytics.errors.last for python details




    def execute_in_api_context_decorator(function):
        return decorator(execute_in_api_context, function)

    return execute_in_api_context_decorator
