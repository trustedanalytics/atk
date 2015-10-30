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
Post Processing of logistic regression train results
"""
try:
    import pandas as pd

    pandas_available = True
except ImportError:
    pandas_available = False


class LogisticRegressionSummary(object):
    """ Defines the results of training a logistic regression model  """

    def __init__(self, json_result):
        from trustedanalytics import get_frame

        self.num_features = json_result['num_features']
        self.num_classes = json_result['num_classes']
        self.covariance_matrix = None

        if pandas_available:
            self.summary_table = pd.DataFrame(data=json_result['coefficients'].values(),
                                              columns=['coefficients'],
                                              index=json_result['coefficients'].keys())
            self.summary_table['degrees_freedom'] = pd.Series(data=json_result['degrees_freedom'].values(),
                                                              index=json_result['degrees_freedom'].keys())

            if json_result.get('covariance_matrix', None) is not None:
                self.summary_table['standard_errors'] = pd.Series(data=json_result['standard_errors'].values(),
                                                                  index=json_result['standard_errors'].keys())
                self.summary_table['wald_statistic'] = pd.Series(data=json_result['wald_statistic'].values(),
                                                                 index=json_result['wald_statistic'].keys())
                self.summary_table['p_value'] = pd.Series(data=json_result['p_value'].values(),
                                                          index=json_result['p_value'].keys())
                self.covariance_matrix = get_frame(json_result['covariance_matrix']['uri'])
        else:
            self.summary_table = {
                'coefficients': json_result['coefficients'],
                'degrees_freedom': json_result['degrees_freedom']
            }
            if json_result.get('covariance_matrix', None) is not None:
                self.summary_table['standard_errors'] = json_result['standard_errors']
                self.summary_table['wald_statistic'] = json_result['wald_statistic']
                self.summary_table['p_value'] = json_result['p_value']
                self.summary_table['covariance_matrix'] = get_frame(json_result['covariance_matrix']['uri'])

    def __repr__(self):
        return "num_features: {0}\nnum_classes: {1}\nsummary_table:\n {2}\ncovariance_matrix:\n {3}".format(
            self.num_features,
            self.num_classes,
            self.summary_table,
            self.covariance_matrix)
