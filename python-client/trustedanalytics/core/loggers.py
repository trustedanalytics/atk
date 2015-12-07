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
Logging - simple helpers for now
"""
import logging
import sys
import inspect

# Constants
API_LOGGER_NAME = 'ATK Python API'
HTTP_LOGGER_NAME = 'trustedanalytics.rest.http'
LINE_FORMAT = '%(asctime)s|%(name)s|%(levelname)-5s|%(message)s'
API_LINE_FORMAT = '|api| %(message)s'

# add a null handler to root logger to avoid handler warning messages
class NullHandler(logging.Handler):
    name = "NullHandler"

    def emit(self, record):
        pass

# this line avoids the 'no handler' warning msg when no logging is set at all
_null_handler = NullHandler()
_null_handler.name = ''  # add name explicitly for python 2.6
logging.getLogger('').addHandler(_null_handler)


class Loggers(object):
    """
    Collection of loggers to stderr, wrapped for simplicity
    """
    # todo - WIP, this will get more sophisticated!

    # map first character of level to actual level setting, for convenience
    _level_map = {'c': logging.CRITICAL,
                  'f': logging.FATAL,
                  'e': logging.ERROR,
                  'w': logging.WARN,
                  'i': logging.INFO,
                  'd': logging.DEBUG,
                  'n': logging.NOTSET}

    def __init__(self):
        self._user_logger_names = []

    def __repr__(self):
        header = ["{0:<8}  {1:<50}  {2:<14}".format("Level", "Logger", "# of Handlers"),
                  "{0:<8}  {1:<50}  {2:<14}".format("-"*8, "-"*50, "-"*14)]
        entries = []
        for name in self._user_logger_names:
            entries.append(self._get_repr_line(name, None))
        return "\n".join(header + entries)

    @staticmethod
    def _get_repr_line(name, alias):
        logger = logging.getLogger(name)
        if alias:
            name += " (%s)" % alias
        return "{0:<8}  {1:<50}  {2:<14}".format(logging.getLevelName(logger.level),
                                                 name,
                                                 len(logger.handlers))

    def set_http(self, level=logging.DEBUG, output=None):
        """
        Sets the level of logging for http traffic

        Parameters
        ----------
        level : int, str or logging.*, optional
            The level to which the logger will be set.  May be 0,10,20,30,40,50
            or "DEBUG", "INFO", etc.  (only first letter is requirecd)
            Setting to None disables the logging to stderr
            See `https://docs.python.org/2/library/logging.html`
            If not specified, DEBUG is used
            To turn OFF the logger, set level to 0 or None
        output: file or str, or list of such, optional
            The file object or name of the file to log to.  If empty, then stderr is used

        Examples
        --------
        >>> loggers.set_http()       # Enables http logging to stderr
        >>> loggers.set_http(None)   # Disables http logging
        """
        self.set(level, HTTP_LOGGER_NAME, output)

    def set_api(self, level=logging.INFO, output=sys.stdout, line_format=API_LINE_FORMAT, verbose=True):
        """
        Sets the level of logging for py api calls (command tracing)

        Parameters
        ----------
        level : int, str or logging.*, optional
            The level to which the logger will be set.  May be 0,10,20,30,40,50
            or "DEBUG", "INFO", etc.  (only first letter is requirecd)
            Setting to None disables the logging to stderr
            See `https://docs.python.org/2/library/logging.html`
            If not specified, INFO is used
            To turn OFF the logger, set level to 0 or None
        output: file or str, or list of such, optional
            The file object or name of the file to log to.  If empty, then stdout is used
        line_format: str, optional
            By default, the api logger has a simple format suitable for interactive use
            To get the standard logger formatting with timestamps, etc., set line_format=None
        verbose: bool, optional
            If False, then only the name of the api command is printed.  It provides much simpler,
            cleaner logging if you're only interested in the command sequence.

        Examples
        --------
        >>> loggers.set_api()       # Enables api logging to stdout
        >>> loggers.set_api(None)   # Disables api logging
        """
        ApiLogFormat.verbose = verbose
        self.set(level, API_LOGGER_NAME, output, line_format)

    def set(self, level=logging.DEBUG, logger_name='', output=None, line_format=None):
        """
        Sets the level and adds handlers to the given logger

        Parameters
        ----------
        level : int, str or logging.*, optional
            The level to which the logger will be set.  May be 0,10,20,30,40,50
            or "DEBUG", "INFO", etc.  (only first letter is requirecd)
            Setting to None disables the logging to stderr
            See `https://docs.python.org/2/library/logging.html`
            If not specified, DEBUG is used
            To turn OFF the logger, set level to 0 or None
        logger_name: str, optional
            The name of the logger.  If empty string, then the trustedanalytics root logger is set
        output: file or str, or list of such, optional
            The file object or name of the file to log to.  If empty, then stderr is used

        Examples
        --------
        # to enable INFO level logging to file 'log.txt' and no printing to stderr:
        >>> loggers.set('INFO', 'trustedanalytics.rest.frame','log.txt', False)
        """
        logger_name = logger_name if logger_name != 'root' else ''
        if not level:
            return self._turn_logger_off(logger_name)

        line_format = line_format if line_format is not None else LINE_FORMAT
        logger = logging.getLogger(logger_name)
        if not output:
            output = sys.stderr
        if isinstance(output, basestring):
            handler = logging.FileHandler(output)
        elif isinstance(output, list) or isinstance(output, tuple):
            logger = None
            for o in output:
                logger = self.set(level, logger_name, o, line_format)
            return logger
        else:
            try:
                handler = logging.StreamHandler(output)
            except:
                raise ValueError("Bad output argument %s.  Expected stream or file name." % output)

        try:
            handler_name = output.name
        except:
            handler_name = str(output)

        if isinstance(level, basestring):
            c = str(level)[0].lower()  # only require first letter
            level = self._level_map[c]
        logger.setLevel(level)

        self._add_handler_to_logger(logger, handler, handler_name, line_format)

        # store logger name
        if logger_name not in self._user_logger_names:
            self._user_logger_names.append(logger_name)
        return logger

    @staticmethod
    def _logger_has_handler(logger, handler_name):
        return logger.handlers and any([h.name for h in logger.handlers if h.name == handler_name])

    @staticmethod
    def _add_handler_to_logger(logger, handler, handler_name, line_format):
        handler.setLevel(logging.DEBUG)
        handler.name = handler_name
        formatter = logging.Formatter(line_format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def _turn_logger_off(self, logger_name):
        logger = logging.getLogger(logger_name)
        logger.level = logging.CRITICAL
        victim_handlers = [x for x in logger.handlers]
        for h in victim_handlers:
            logger.removeHandler(h)
        try:
            self._user_logger_names.remove(logger_name)
        except ValueError:
            pass
        return logger

loggers = Loggers()


# API logger
_api_logger = logging.getLogger(API_LOGGER_NAME)


def log_api_call(call_depth, function, *args, **kwargs):
    """calls api logger if enabled; needs a call_depth, i.e. the number of call frames from the actual calling line"""
    if 0 < _api_logger.level <= logging.INFO:
        try:
            frame, filename, line_number, function_name, lines, index = inspect.stack()[call_depth]
            #try:   # at one point we were printing the line itself, this is how
            #    line = lines[index].rstrip()
            #except:
            #    line = "(unable to determine line contents)"
            location = "%s[%s]" % (filename, line_number)
        except:
            location = "(unknown location)"
        _api_logger.info(ApiLogFormat.format_call(location, function, *args, **kwargs))


class ApiLogFormat(object):
    """Formats api calls for logging"""

    verbose = True  # global setting

    @staticmethod
    def format_call(location, function, *args, **kwargs):
        try:
            full_name = function.command.full_name
        except:
            full_name = function.__name__

        if not ApiLogFormat.verbose:
            return full_name
        try:
            param_names = function.func_code.co_varnames[0:function.func_code.co_argcount]
            named_args = zip(param_names, args)
            self = None
            try:
                if param_names[0] == "self":
                    self = args[0]
                    named_args = named_args[1:]
            except:
                self = None
            named_args.extend(kwargs.items())
            formatted_args = '' if not named_args else "(%s)" % (", ".join([ApiLogFormat.format_kwarg(k,v) for k, v in named_args]))
            is_constructor = function.__name__ == '__init__' or function.__name__ == 'new'
            formatted_self = ApiLogFormat.format_self(self) if not is_constructor else (ApiLogFormat._format_entity(self) + '.') if self is not None else '<None?>.'

            return "%s %s %s%s%s" % (location, full_name, formatted_self, ApiLogFormat.format_function(function), formatted_args)
        except Exception as e:
            return str(e)

    @staticmethod
    def format_kwarg(name, value):
        return "%s=%s" % (name, ApiLogFormat.format_value(value))

    @staticmethod
    def format_function(f):
        return f.__name__ if f.__name__ != '__name' else 'name'  # special case for '__name'

    @staticmethod
    def format_self(v):
        return (ApiLogFormat.format_value(v) + ".") if v is not None else ''

    @staticmethod
    def format_value(v):
        if hasattr(v, "uri"):  # the tell of an entity
            return ApiLogFormat._format_entity(v)
        return repr(v)

    @staticmethod
    def _format_entity(entity):
        return "<%s:%s>" % (type(entity).__name__, hex(id(entity))[2:])


# Logging backdoor
#
# If env variable is set, we will call loggers.set immediately, so the loggers
# can run during the rest of the trustedanalytics package import
#
# The value of this env var is a JSON list containing map, each of which
# represents a call to loggers.set.  The map holds the **kwargs for the
# call to loggers.set
#
# Example:  This sets the module logger to debug for core/frame.py
#
# $ export TRUSTEDANALYTICS_LOGGERS='[{"logger_name": "trustedanalytics.core.frame", "level": "debug"}]'
#
import os
loggers_set_env_name = "TRUSTEDANALYTICS_LOGGERS"
loggers_set_env = os.getenv(loggers_set_env_name)
if loggers_set_env:
    try:
        import json
        for entry in json.loads(loggers_set_env):
            loggers.set(**entry)
    except Exception as e:
        import sys
        sys.stderr.write("!! Error trying to ingest logging env variable $%s\n" % loggers_set_env_name)
        raise
