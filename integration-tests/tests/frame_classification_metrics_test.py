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

import unittest
import trustedanalytics as ta

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class FrameClassificationTest(unittest.TestCase):
    """
    Test classification_metrics()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_classification_metrics_001(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/classification-compute.csv", schema= [('a', str),
                                            ('b', ta.int32),
                                            ('labels', ta.int32),
                                            ('predictions', ta.int32)], delimiter=',', skip_header_lines=1)

        print "create frame"
        frame = ta.Frame(csv)

        self.assertEquals(frame.row_count, 4, "frame should have 4 rows")
        self.assertEqual(frame.column_names, ['a', 'b', 'labels', 'predictions'])

        print "compute classification_metrics()"
        cm = frame.classification_metrics('labels', 'predictions', 1, 1)

        self.assertEquals(cm.f_measure, 0.66666666666666663, "computed f_measure for this model should be equal to 0.66666666666666663")
        self.assertEquals(cm.recall, 0.5, "computed recall for this model should be equal to 0.5")
        self.assertEquals(cm.accuracy, 0.75, "computed accuracy for this model should be equal to 0.75")
        self.assertEquals(cm.precision, 1.0, "computed precision for this model should be equal to 1.0")

        confusion_matrix = cm.confusion_matrix.values.tolist()
        self.assertEquals(confusion_matrix, [[1, 1], [0, 2]], "computed confusion_matrix for this models should be equal to [[1, 1], [0, 2]]")


if __name__ == "__main__":
    unittest.main()
