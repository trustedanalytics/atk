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

from trustedanalytics.meta.doc import DocExamplesPreprocessor, DocExamplesException

class TestDoc(unittest.TestCase):

    def test_examples_preprocessor(self):

        examples = """
<hide>
>>> import trustedanalytics as ta  # invisible set up code
>>> ta.connect()
<connect>
</hide>

Blah Blah
<hide>
More mysteries
<and>
Disguises
 </hide>

<skip>
>>> code that doesn't test well
<progress>
</skip>
end
"""
        expected_doc = """

Blah Blah

>>> code that doesn't test well
[===Job Progress===]
end
"""

        expected_doctest = """
>>> import trustedanalytics as ta  # invisible set up code
>>> ta.connect()
-etc-

Blah Blah
More mysteries
<and>
Disguises

end
"""

        results = DocExamplesPreprocessor(examples, mode='doc')
        self.assertEqual(expected_doc, str(results))

        results = DocExamplesPreprocessor(examples, mode='doctest')
        self.assertEqual(expected_doctest, str(results))

    def test_examples_preprocessor_neg(self):

        unclosed = """
    <hide>
    >>> I'm invisible set up code that forgot to close the tag properly
    <unhide>

    Blah Blah
    """
        try:
            DocExamplesPreprocessor(unclosed)
        except DocExamplesException as e:
            self.assertTrue("unclosed tag" in str(e))
        else:
            self.fail("Expected exception")

        nested = """
    <hide>
    >>> I'm invisible set up code that forgot to close the tag properly
    <hide>
    >>> really trying to hide this
    </hide>

    Blah Blah
    """
        try:
            DocExamplesPreprocessor(nested)
        except DocExamplesException as e:
            self.assertTrue("nested tag" in str(e))
        else:
            self.fail("Expected exception")

        unexpected = """
    >>> I'm invisible set up code that forgot to close the tag properly
    </hide>

    Blah Blah
    """
        try:
            DocExamplesPreprocessor(unexpected)
        except DocExamplesException as e:
            self.assertTrue("unexpected tag" in str(e))
        else:
            self.fail("Expected exception")

        # take None input
        self.assertEqual('', str(DocExamplesPreprocessor(None)))


if __name__ == '__main__':
    unittest.main()
