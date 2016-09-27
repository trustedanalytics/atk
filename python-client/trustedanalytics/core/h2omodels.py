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
    """object
     An object with the results of the trained Random Forest Regressor:
     |'value_column': the column name containing the value of each observation,
     |'observation_columns': the list of observation columns on which the model was trained,
     |'num_trees': the number of decision trees in the random forest,
     |'max_depth': the maximum depth of the tree,
     |'num_bins': for numerical columns, build a histogram of at least this many bins
     |'min_rows': number of features to consider for splits at each node
     |'feature_subset_category': number of features to consider for splits at each node,
     |'tree_stats': dictionary with tree statistics for trained model,
     |'varimp': variable importances
     |'seed': the random seed used for bootstrapping and choosing feature subset,
     |'sample_rate': row sample rate per tree
   """
    def __init__(self, json_result):
        self.value_column = json_result['value_column']
        self.observation_columns = json_result['observation_columns']
        self.num_trees = json_result['num_trees']
        self.max_depth = json_result['max_depth']
        self.num_bins = json_result['num_bins']
        self.min_rows = json_result['min_rows']
        self.feature_subset_category = json_result['feature_subset_category']
        self.tree_stats = json_result['tree_stats']
        self.varimp = json_result['varimp']  #Variable importances

    def __repr__(self):
        return "value_column: {0}\nobservation_columns: {1}\nnum_trees: {2}\nmax_depth: {3}\nnum_bins: {4}\nmin_rows: {5}\nfeature_subset_category: {6}\ntree_stats: \n{7}\nvarimp: \n{8}".format(
            self.value_column, self.observation_columns, self.num_trees, self.max_depth, self.num_bins, self.min_rows,
            self.feature_subset_category, self.tree_stats, self.varimp)

    def varimp_as_pandas(self):
        """get the Variable Importances as a pandas DataFrame"""
        import pandas as pd
        return pd.DataFrame(self.varimp.items(), columns=['variable', 'importance'])


class H2oRandomForestRegressorTestResult(object):
    """ Defines the results for H2O random forest regression test  """

    def __init__(self, json_result):
        self.mae = json_result['mae'] # Mean absolute error
        self.mse = json_result['mse']   #Mean squared error
        self.rmse = json_result['rmse'] #Root mean squared error
        self.r2 = json_result['r_2']     #R-squared or coefficient of determination
        self.explained_variance_score = json_result['explained_variance_score']

    def __repr__(self):
        return "mae: {0}\nmse: {1}\nrmse: {2}\nr2: {3}\nexplained_variance_score: {4}".format(self.mae, self.mse, self.rmse, self.r2, self.explained_variance_score)

