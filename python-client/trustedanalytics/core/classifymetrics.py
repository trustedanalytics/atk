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
Post Processing of classification metrics results
"""
import pandas as pd

class ClassificationMetricsResult(object):
    """ Defines the results for binary and multi class classification metrics  """

    def __init__(self, json_result):
        self.precision = json_result['precision']
        self.f_measure = json_result['f_measure']
        self.accuracy = json_result['accuracy']
        self.recall = json_result['recall']
        cm_result = json_result['confusion_matrix']
        if cm_result:
            header = map (lambda x: "Predicted_" + x.title(),  cm_result['column_labels'])
            row_index = map (lambda x: "Actual_" + x.title(),  cm_result['row_labels'])
            #data = [item for row in cm_result['matrix'] for item in row]
            data = cm_result['matrix']
            self.confusion_matrix = pd.DataFrame(data, index=row_index, columns=header)
        else:
            #empty pandas frame
            pd.DataFrame()

    def __repr__(self):
        return "Precision: {0}\nRecall: {1}\nAccuracy: {2}\nFMeasure: {3}\nConfusion Matrix: \n{4}".format(self.precision, self.recall, self.accuracy, self.f_measure, self.confusion_matrix)
