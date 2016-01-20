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

import iatest
iatest.init()

import unittest
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


if __name__ == '__main__':
    unittest.main()
