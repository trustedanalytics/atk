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
trustedanalytics package init, public API
"""
import sys

if not sys.version_info[:2] == (2, 7):
    raise EnvironmentError("Python 2.7 required.  Detected version: %s.%s.%s" % tuple(sys.version_info[:3]))
del sys

from trustedanalytics.core.loggers import loggers
from trustedanalytics.core.atktypes import *
from trustedanalytics.core.aggregation import agg
from trustedanalytics.core.errorhandle import errors
from trustedanalytics.core.files import CsvFile, LineFile, JsonFile, MultiLineFile, XmlFile, HiveQuery, HBaseTable, JdbcTable, UploadRows, OrientDBGraph
from trustedanalytics.core.atkpandas import Pandas
from trustedanalytics.rest.udfdepends import udf # todo: deprecated, pls. remove
from trustedanalytics.core.frame import Frame, VertexFrame
from trustedanalytics.core.graph import Graph
from trustedanalytics.core.model import _BaseModel
from trustedanalytics.core.ui import inspect_settings
from trustedanalytics.core.admin import release


from trustedanalytics.rest.atkserver import server
connect = server.connect


try:
    from trustedanalytics.core.docstubs2 import *
except Exception as e:
    errors._doc_stubs = e
    del e

# do api_globals last because other imports may have added to the api_globals



def _refresh_api_namespace():
    from trustedanalytics.core.api import api_globals
    for item in api_globals:
        #globals()[item.__name__] = item
        if item.__name__ == "H2oRandomForestRegressorPrivateModel":
            from trustedanalytics.meta.metaprog import set_installation, CommandInstallation
            class H2oRandomForestRegressorModel(item):
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
            globals()["H2oRandomForestRegressorModel"] = H2oRandomForestRegressorModel

        else:
            globals()[item.__name__] = item


    del api_globals

_refresh_api_namespace()


def _get_api_info():
    """Gets the set of all the command full names in the API"""
    from trustedanalytics.meta.installapi import ApiInfo
    import sys
    return ApiInfo(sys.modules[__name__])


def _get_server_api_raw():
    """Gets the raw metadata from the server concerning the command API"""
    from trustedanalytics.meta.installapi import ServerApiRaw
    return ServerApiRaw(server)


def _walk_api(cls_function, attr_function, include_init=False):
    """Walks the installed API and runs the given functions for class and attributes in the API"""
    from trustedanalytics.meta.installapi import walk_api
    import sys
    return walk_api(sys.modules[__name__], cls_function, attr_function, include_init=include_init)


from trustedanalytics.core.api import api_status
from trustedanalytics.rest.atkserver import create_credentials_file

from trustedanalytics.core.datacatalog import data_catalog


from trustedanalytics.rest.udfzip import UdfDependencies
udf_dependencies = UdfDependencies([])
del UdfDependencies


version = None  # This client build ID value is auto-filled during packaging.  Set to None to disable check with server
