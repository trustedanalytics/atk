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


def get_H2oRandomForestRegressorModel_installation(baseclass):
    """Creates a new class wrapper for the H2O python class generated by the meta-programming layer"""

    from trustedanalytics.meta.metaprog import set_installation, CommandInstallation

    class H2oRandomForestRegressorModel(baseclass):
        """
        Wrapper class for H2O random forest regression
        :return:
        """
        def train(self, frame, value_column, observation_columns, num_trees=50, max_depth=20, num_bins=20, min_rows=10, feature_subset_category='auto', seed=None, sample_rate=None):
            """
            Build H2O Random Forests Regressor model using the observation columns and target column.

            H2O's implementation of distributed random forest is slow for large trees due to the
            overhead of shipping histograms across the network. This plugin runs H2O random forest
            in a single node for small datasets, and multiple nodes for large datasets.

            :param frame: A frame to train the model on
            :type frame: Frame
            :param value_column: Column name containing the value for each observation
            :type value_column: unicode
            :param observation_columns: Column(s) containing the observations
            :type observation_columns: list
            :param num_trees: (default=50)  Number of trees in the random forest.
            :type num_trees: int32
            :param max_depth: (default=20)  Maximum depth of the tree.
            :type max_depth: int32
            :param num_bins: (default=20)  For numerical columns (real/int), build a histogram of (at least) this many bins.
            :type num_bins: int32
            :param min_rows: (default=10)  Minimum number of rows to assign to terminal nodes.
            :type min_rows: int32
            :param feature_subset_category: (default=auto)  Number of features to consider for splits at each node. Supported values "auto", "all", "sqrt", "onethird".
                If "auto" is set, this is based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1, set to "onethird".
            :type feature_subset_category: unicode
            :param seed: (default=None)  Seed for random numbers (affects sampling) - Note: only reproducible when running single threaded.
            :type seed: int32
            :param sample_rate: (default=None)  Row sample rate per tree (from 0.0 to 1.0).
            :type sample_rate: float64

            :returns: object
                      An object with the results of the trained Random Forest Regressor:
                      'value_column': the column name containing the value of each observation,
                      'observation_columns': the list of observation columns on which the model was trained,
                      'num_trees': the number of decision trees in the random forest,
                      'max_depth': the maximum depth of the tree,
                      'num_bins': for numerical columns, build a histogram of at least this many bins
                      'min_rows': number of features to consider for splits at each node
                      'feature_subset_category': number of features to consider for splits at each node,
                      'tree_stats': dictionary with tree statistics for trained model,
                      'varimp': variable importances

            :rtype: object
            """
            from trustedanalytics.rest.command import CommandRequest, executor
            from trustedanalytics.core.admin import release
            from trustedanalytics.core.h2omodels import H2oRandomForestRegressorTrainResult
            arguments = {'model' : self.uri,
                         'frame' : frame.uri,
                         'value_column': value_column,
                         'observation_columns': observation_columns,
                         'num_trees': num_trees,
                         'max_depth': max_depth,
                         'num_bins': num_bins,
                         'min_rows': min_rows,
                         'feature_subset_category': feature_subset_category,
                         'seed': seed,
                         'sample_rate': sample_rate}
            release()
            print "Training H2O random forest regression model..."
            if frame.row_count < 100000:
                result = executor.execute("model:h2o_random_forest_regressor_private/_local_train", item, arguments)
            else:
                result = executor.execute("model:h2o_random_forest_regressor_private/_distributed_train", item, arguments)
            release()
            return H2oRandomForestRegressorTrainResult(result)

    set_installation(H2oRandomForestRegressorModel, CommandInstallation("model:h2o_random_forest_regressor_private", host_class_was_created=True))
    return H2oRandomForestRegressorModel
