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
Command objects.
"""
import time
import json
import logging
import re
import sys  # for unit tests

from requests import HTTPError

logger = logging.getLogger(__name__)

import trustedanalytics.rest.config as config
from trustedanalytics.rest.atkserver import server
from trustedanalytics.rest.progress import ProgressPrinter, ProgressPrinterOld
from collections import namedtuple
from spark import IaPyWorkerError

def execute_command(command_name, selfish, **arguments):
    """Executes command and returns the output."""
    command_request = CommandRequest(command_name, arguments)
    command_info = executor.issue(command_request)
    result = command_info.result
    return result


class CommandRequest(object):
    """Represents a command request --the command name and its arguments"""

    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments

    def to_json_obj(self):
        """
        Returns json for REST payload.
        """
        return self.__dict__


class CommandInfo(object):
    """Abstracts the JSON representation of the command from the server"""

    __commands_regex = re.compile("""^http.+/(commands)/\d+""")

    @staticmethod
    def is_valid_command_uri(uri):
        return CommandInfo.__commands_regex.match(uri) is not None

    def __init__(self, response_payload):
        self._payload = response_payload
        if not self.is_valid_command_uri(self.uri):
            raise ValueError("Invalid command URI: " + self.uri)

    def __repr__(self):
        return json.dumps(self._payload, indent=2, sort_keys=True)

    def __str__(self):
        return 'commands/%s "%s"' % (self.id_number, self.name)

    @property
    def id_number(self):
        return self._payload['id']

    @property
    def correlation_id(self):
        return self._payload.get('correlation_id', None)

    @property
    def name(self):
        return self._payload.get('name', None)

    @property
    def uri(self):
        try:
            return self._payload['links'][0]['uri']
        except KeyError:
            return ""

    @property
    def error(self):
        try:
            return self._payload['error']
        except KeyError:
            return None

    @property
    def complete(self):
        try:
            return self._payload['complete']
        except KeyError:
            return False

    @property
    def result(self):
        try:
            return self._payload['result']
        except KeyError:
            return False

    @property
    def progress(self):
        try:
            return self._payload['progress']
        except KeyError:
            return False

    def update(self, payload):
        if self._payload and self.id_number != payload['id']:
            msg = "Invalid payload, command ID mismatch %d when expecting %d"\
                  % (payload['id'], self.id_number)
            logger.error(msg)
            raise RuntimeError(msg)
        self._payload = payload


class Polling(object):

    @staticmethod
    def poll(uri, predicate=None, start_interval_secs=None, max_interval_secs=None, backoff_factor=None):
        """
        Issues GET methods on the given command uri until the response
        command_info cause the predicate to evalute True.  Exponential retry
        backoff

        Parameters
        ----------
        uri : str
            The uri of the command
        predicate : function
            Function with a single CommandInfo parameter which evaluates to True
            when the polling should stop.
        start_interval_secs : float
            Initial sleep interval for the polling, in seconds
        max_interval_secs : float
            Maximum sleep interval for the polling, in seconds
        backoff_factor : float
            Factor to increase the sleep interval on subsequent retries
        """
        if predicate is None:
            predicate = Polling._get_completion_status
        if start_interval_secs is None:
            start_interval_secs = config.polling_defaults.start_interval_secs
        if backoff_factor is None:
            backoff_factor = config.polling_defaults.backoff_factor
        if max_interval_secs is None:
            max_interval_secs = config.polling_defaults.max_interval_secs
        if not CommandInfo.is_valid_command_uri(uri):
            raise ValueError('Cannot poll ' + uri + ' - a /commands/{number} uri is required')
        interval_secs = start_interval_secs
        command_info = None
        printer = ProgressPrinter()
        start_time = time.time()
        while True:
            command_info = Polling._get_command_info(uri)
            finish = predicate(command_info)
            progress = command_info.progress
            printer.update(progress)

            if finish:
                break

            #if len(last_progress) == len(progress) and interval_secs < max_interval_secs:
            #    interval_secs = min(max_interval_secs, interval_secs * backoff_factor)

            #last_progress = progress
            uri = command_info.uri
            time.sleep(interval_secs)

        logger.info("polling %s completed after %0.3f seconds" % (uri, time.time() - start_time))
        if not command_info.error:
            printer.update("Done [=========================] 100.00%", True)
        return command_info

    @staticmethod
    def _get_command_info(uri):
        response = server.get(uri)
        return CommandInfo(response.json())

    @staticmethod
    def _get_completion_status(command_info):
        return command_info.complete


class OperationCancelException(Exception):
    pass

class CommandServerError(Exception):
    """
    Error for errors reported by the server when issuing a command
    """
    def __init__(self, command_info):
        self.command_info = command_info
        try:
            message = command_info.error['message']
            message = message + " " + self.parse_stack_trace(command_info)
        except KeyError:
            message = "(Server response insufficient to provide details)"
        message = message + (" (command: %s, corId: %s)" % (command_info.id_number, command_info.correlation_id))
        Exception.__init__(self, message)

    @staticmethod
    def parse_stack_trace(command_info):
        """
        Get detailed error message from stack trace
        """
        try:
            return re.search(r'%s:\s*(.*)\n' % IaPyWorkerError.__name__, command_info.error['stack_trace']).group(1)
        except:
            return ""


QueryResult = namedtuple("QueryResult", ['data', 'schema'])
"""
QueryResult contains the data and schema directly returned from the rest server
"""

class Executor(object):
    """
    Executes commands
    """

    def get_query_response(self, id, partition):
        """
        Attempt to get the next partition of data as a CommandInfo Object. Allow for several retries
        :param id: Query ID
        :param partition: Partition number to pull
        """
        max_retries = 20
        for i in range(max_retries):
            try:
                info = CommandInfo(server.get("queries/%s/data/%s" % (id, partition)).json())
                return info
            except HTTPError as e:
                time.sleep(5)
                if i == max_retries - 1:
                    raise e

    def issue(self, command_request):
        """
        Issues the command_request to the server
        """
        logger.info("Issuing command " + command_request.name)
        response = server.post("commands", command_request.to_json_obj())
        return self.poll_command_info(response)

    def poll_command_info(self, response):
        """
        poll command_info until the command is completed and return results

        :param response: response from original command
        :rtype: CommandInfo
        """
        command_info = CommandInfo(response.json())
        # For now, we just poll until the command completes
        try:
            if not command_info.complete:
                command_info = Polling.poll(command_info.uri)

        except KeyboardInterrupt:
            self.cancel(command_info.id_number)
            raise OperationCancelException("command cancelled by user")

        if command_info.error:
            raise CommandServerError(command_info)
        return command_info

    def query(self, query_url):
        """
        Issues the query_request to the server
        """
        logger.info("Issuing query " + query_url)
        try:
            response = server.get(query_url)
        except:
            # do a single retry
            response = server.get(query_url)

        response_json = response.json()

        schema = response_json["result"]["schema"]['columns']

        if response_json["complete"]:
            data = response_json["result"]["data"]
            return QueryResult(data, schema)
        else:
            command = self.poll_command_info(response)

            #retreive the data
            printer = ProgressPrinterOld()
            total_pages = command.result["total_pages"] + 1

            start = 1
            data = []
            for i in range(start, total_pages):
                next_partition = self.get_query_response(command.id_number, i)
                data_in_page = next_partition.result["data"]

                data.extend(data_in_page)

                #if the total pages is greater than 10 display a progress bar
                if total_pages > 5:
                    finished = i == (total_pages - 1)
                    if not finished:
                        time.sleep(.5)
                    progress = [{
                        "progress": (float(i)/(total_pages - 1)) * 100,
                        "tasks_info": {
                            "retries": 0
                        }
                    }]
                    printer.print_progress(progress, finished)
        return QueryResult(data, schema)

    def cancel(self, command_id):
        """
        Tries to cancel the given command
        """
        logger.info("Executor cancelling command " + str(command_id))

        arguments = {'status': 'cancel'}
        command_request = CommandRequest("", arguments)
        server.post("commands/%s" %(str(command_id)), command_request.to_json_obj())

    def execute(self, command_name, selfish, arguments):
        """
        Executes command and returns the output.
        :param command_name: command full name
        :param selfish: the entity instance for the command
        :param arguments: dict (json-friendly) of the arguments
        """
        return execute_command(command_name, selfish, **arguments)


executor = Executor()
