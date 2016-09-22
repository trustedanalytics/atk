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
Post Processing of H2O model results
"""

class H2oRandomForestRegressorTrainResult(object):
    """ Defines the results for H2O random forest regression training  """

    def __init__(self, json_result):
        self.mse = json_result['mse']   #Mean squared error
        self.rmse = json_result['rmse'] #Root mean squared error
        self.r2 = json_result['r_2']     #R-squared or coefficient of determination
        self.varimp = json_result['varimp']  #Variable importances

    def __repr__(self):
        return "mse: {0}\nrmse: {1}\nr2: {2}\nvarimp: \n{3}".format(self.mse, self.rmse, self.r2, self.varimp)

    def varimp_as_pandas(self):
        """get the Variable Importances as a pandas DataFrame"""
        import pandas as pd
        return pd.DataFrame(self.varimp.items(), columns=['variable', 'importance'])


class H2oRandomForestRegressorTestResult(object):
    """ Defines the results for H2O random forest regression test  """

    def __init__(self, json_result):
        self.mse = json_result['mse']   #Mean squared error
        self.rmse = json_result['rmse'] #Root mean squared error
        self.r2 = json_result['r_2']     #R-squared or coefficient of determination

    def __repr__(self):
        return "mse: {0}\nrmse: {1}\nr2: {2}".format(self.mse, self.rmse, self.r2)

