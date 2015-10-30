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

from trustedanalytics.meta.doc import Doc

class DocExtraction(unittest.TestCase):

    doc1 = """Computes a cumulative percent sum.

    A cumulative percent sum is computed by sequentially stepping through the
    column values and keeping track of the current percentage of the total sum
    accounted for at the current value."""

    def test_get_from_str(self):
        d = Doc.get_from_str(self.doc1)
        self.assertEqual("Computes a cumulative percent sum.", d.one_line)
        self.assertEqual("""A cumulative percent sum is computed by sequentially stepping through the
column values and keeping track of the current percentage of the total sum
accounted for at the current value.""", d.extended)

if __name__ == '__main__':
    unittest.main()
