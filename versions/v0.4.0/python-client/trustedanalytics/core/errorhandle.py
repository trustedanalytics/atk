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

import sys
import traceback
import warnings


class Errors(object):
    """
    Settings and methods for Python API layer error handling
    """
    def __init__(self):
        self._show_details = False
        self._last_exc_info = None  # the last captured API layer exception

    def __repr__(self):
        return "show_details = %s\nlast = %s" % (self._show_details, self.last)

    _help_msg = """(For full stack trace of this error, use: errors.last
 To always show full details, set errors.show_details = True)
"""

    @property
    def show_details(self):
        """Boolean which determines if the full exception traceback is included in the exception messaging"""
        return self._show_details

    @show_details.setter
    def show_details(self, value):
        self._show_details = value

    @property
    def last(self):
        """String containing the details (traceback) of the last captured exception"""
        last_exception = self._get_last()
        return ''.join(last_exception) if last_exception else None

    def _get_last(self):
        """Returns list of formatted strings of the details (traceback) of the last captured exception"""
        if self._last_exc_info:
            (exc_type, exc_value, exc_tb) = self._last_exc_info
            return traceback.format_exception(exc_type, exc_value, exc_tb)
        else:
            return None

# singleton
errors = Errors()


class IaError(Exception):
    """
    Internal Error Factory for the API layer to report or remove error stack trace.

    Use with raise

    Examples
    --------
    >>> try:
    ...    x = 4 / 0  # some work...
    ... except:
    ...    raise IaError()
    """
    def __new__(cls, logger=None):
        exc_info = sys.exc_info()
        errors._last_exc_info = exc_info
        try:
            cls.log_error(logger)
        except:
            warnings.warn("Unable to log exc_info", RuntimeWarning)

        if errors.show_details:
            # to show the stack, we just re-raise the last exception as is
            raise
        else:
            # to hide the stack, we return the exception info w/o trace
            #sys.stderr.write(errors._help_msg)
            #sys.stderr.flush()
            return exc_info[1], None, None

    @classmethod
    def log_error(cls, logger=None):
        if logger:
            message = traceback.format_exc(limit=None)
            logger.error(message)
