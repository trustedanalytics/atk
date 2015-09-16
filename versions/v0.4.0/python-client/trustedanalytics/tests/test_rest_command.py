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

import iatest
iatest.init()

import unittest
from mock import patch, Mock
from trustedanalytics.rest.command import ProgressPrinter
from trustedanalytics.rest.server import HostPortHelper


class TestServer(unittest.TestCase):

    def test_re(self):
        self.assertEquals(('alpha', '1010'), HostPortHelper.get_host_port("alpha:1010"))
        self.assertEquals(('alpha.com', None), HostPortHelper.get_host_port("alpha.com"))
        self.assertEquals(('https://alpha.com', None), HostPortHelper.get_host_port("https://alpha.com"))
        self.assertEquals(('https://alpha.com', '1010'), HostPortHelper.get_host_port("https://alpha.com:1010"))

    def test_set_uri(self):
        self.assertEquals('beta:1011', HostPortHelper.set_uri_host('alpha:1011', 'beta'))
        self.assertEquals('beta:1011', HostPortHelper.set_uri_host('alpha:1011', 'beta'))
        self.assertEquals('alpha', HostPortHelper.set_uri_port('alpha:1010', None))
        self.assertEquals(None, HostPortHelper.set_uri_host('alpha:1010', None))


class TestRestCommand(unittest.TestCase):

    @patch('trustedanalytics.rest.command.sys.stdout')
    def test_print_initialization(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        self.assertEqual(len(write_queue), 1)
        self.assertEqual(write_queue[0], "\rinitializing...")

    @patch('trustedanalytics.rest.command.sys.stdout')
    def test_print_receive_progress(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        printer.print_progress([{"progress": 30.0, "tasks_info": {"retries": 0}}], False)

        self.assertEqual(len(write_queue), 2)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=======..................]  30.00% Tasks retries:0 Time 0:00:00")

    @patch('trustedanalytics.rest.command.sys.stdout')
    def test_print_next_progress(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        printer.print_progress([{"progress": 20.0, "tasks_info": {"retries": 0}}], False)
        self.assertEqual(len(write_queue), 2)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=====....................]  20.00% Tasks retries:0 Time 0:00:00")

        printer.print_progress([{"progress": 50.0, "tasks_info": {"retries": 0}},
                                {"progress": 100.0, "tasks_info": {"retries": 0}}], True)

        self.assertEqual(len(write_queue), 4)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=====....................]  20.00% Tasks retries:0 Time 0:00:00")
        self.assertEqual(write_queue[2], "\r[============.............]  50.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[3], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")

    @patch('trustedanalytics.rest.command.sys.stdout')
    def test_multiple_progress_come_right_after_initializing_stage(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        printer.print_progress([{"progress": 100.0, "tasks_info": {"retries": 0}},
                                {"progress": 100.0, "tasks_info": {"retries": 0}},
                                {"progress": 50.0, "tasks_info": {"retries": 0}}], True)

        self.assertEqual(len(write_queue), 4)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[2], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[3], "\r[============.............]  50.00% Tasks retries:0 Time 0:00:00\n")

    @patch('trustedanalytics.rest.command.sys.stdout')
    def test_multiple_progress_come_as_finished(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        printer.print_progress([{"progress": 30.0, "tasks_info": {"retries": 0}}], False)

        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=======..................]  30.00% Tasks retries:0 Time 0:00:00")
        printer.print_progress([{"progress": 100.0, "tasks_info": {"retries": 0}},
                                {"progress": 100.0, "tasks_info": {"retries": 0}},
                                {"progress": 100.0, "tasks_info": {"retries": 0}}], True)

        self.assertEqual(len(write_queue), 5)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=======..................]  30.00% Tasks retries:0 Time 0:00:00")
        self.assertEqual(write_queue[2], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[3], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[4], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")


if __name__ == '__main__':
    unittest.main()
