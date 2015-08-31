#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import trustedanalytics as atk

# show full stack traces
atk.errors.show_details = True
atk.loggers.set_api()
# TODO: port setup should move to a super class
if atk.server.port != 19099:
    atk.server.port = 19099
atk.connect()

class ApiExistsSmokeTest(unittest.TestCase):
    """
    This test makes sure the API exists.  Sometimes packaging or plugin system bugs might cause
    parts of the API to disappear.  This helps catch it quickly.

    ---

    Smoke test basic frame operations to verify functionality that will be needed by all other tests.

    If these tests don't pass, there is no point in running other tests.

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_expected_methods_exist_on_csvfile(self):
        self.assert_methods_defined(['annotation',
                                    'field_names',
                                    'field_types'], atk.CsvFile)

    def test_expected_methods_exist_on_frame(self):
        self.assert_methods_defined(['add_columns',
                                      'append',
                                      'assign_sample',
                                      'bin_column',
                                      'bin_column_equal_depth',
                                      'bin_column_equal_width',
                                      'classification_metrics',
                                      'column_median',
                                      'column_mode',
                                      'column_names',
                                      'column_summary_statistics',
                                      'copy',
                                      'correlation',
                                      'correlation_matrix',
                                      'count',
                                      'covariance',
                                      'covariance_matrix',
                                      'cumulative_percent',
                                      'cumulative_sum',
                                      'dot_product',
                                      'download',
                                      'drop_columns',
                                      'drop_duplicates',
                                      'drop_rows',
                                      'ecdf',
                                      'entropy',
                                      'export_to_csv',
                                      'export_to_json',
                                      'filter',
                                      'flatten_column',
                                      'get_error_frame',
                                      'group_by',
                                      'histogram',
                                      'inspect',
                                      'join',
                                      'label_propagation', 
                                      'loopy_belief_propagation', 
                                      'name',
                                      'quantiles',
                                      'rename_columns',
                                      'row_count',
                                      'schema',
                                      'sort',
                                      'status',
                                      'take',
                                      'tally',
                                      'tally_percent',
                                      'top_k',
                                      'unflatten_column'], atk.Frame)

    def test_expected_methods_exist_on_vertexframe(self):
        self.assert_methods_defined([ 'add_columns',
                                      'add_vertices',
                                      'assign_sample',
                                      'bin_column',
                                      'bin_column_equal_depth',
                                      'bin_column_equal_width',
                                      'classification_metrics',
                                      'column_median',
                                      'column_mode',
                                      'column_names',
                                      'column_summary_statistics',
                                      'copy',
                                      'correlation',
                                      'correlation_matrix',
                                      'count',
                                      'covariance',
                                      'covariance_matrix',
                                      'cumulative_percent',
                                      'cumulative_sum',
                                      'dot_product',
                                      'download',
                                      'drop_columns',
                                      'drop_duplicates',
                                      'drop_rows',
                                      'drop_vertices',
                                      'ecdf',
                                      'entropy',
                                      'export_to_csv',
                                      'export_to_json',
                                      'filter',
                                      'flatten_column',
                                      'get_error_frame',
                                      'group_by',
                                      'histogram',
                                      'inspect',
                                      'join',
                                      'name',
                                      'quantiles',
                                      'rename_columns',
                                      'row_count',
                                      'schema',
                                      'sort',
                                      'status',
                                      'take',
                                      'tally',
                                      'tally_percent',
                                      'top_k',
                                      'unflatten_column'], atk.VertexFrame)

    def test_expected_methods_exist_on_edgeframe(self):
        self.assert_methods_defined([ 'add_columns',
                                      'add_edges',
                                      'assign_sample',
                                      'bin_column',
                                      'bin_column_equal_depth',
                                      'bin_column_equal_width',
                                      'classification_metrics',
                                      'column_median',
                                      'column_mode',
                                      'column_names',
                                      'column_summary_statistics',
                                      'copy',
                                      'correlation',
                                      'correlation_matrix',
                                      'count',
                                      'covariance',
                                      'covariance_matrix',
                                      'cumulative_percent',
                                      'cumulative_sum',
                                      'dot_product',
                                      'download',
                                      'drop_columns',
                                      'drop_duplicates',
                                      'drop_rows',
                                      'ecdf',
                                      'entropy',
                                      'export_to_csv',
                                      'export_to_json',
                                      'filter',
                                      'flatten_column',
                                      'get_error_frame',
                                      'group_by',
                                      'histogram',
                                      'inspect',
                                      'join',
                                      'name',
                                      'quantiles',
                                      'rename_columns',
                                      'row_count',
                                      'schema',
                                      'sort',
                                      'status',
                                      'take',
                                      'tally',
                                      'tally_percent',
                                      'top_k',
                                      'unflatten_column'], atk.EdgeFrame)

    def test_expected_methods_exist_on_graph(self):
        self.assert_methods_defined(['annotate_degrees',
                                     'annotate_weighted_degrees',
                                     'clustering_coefficient',
                                     'copy',
                                     'define_edge_type',
                                     'define_vertex_type',
                                     'edge_count',
                                     'edges',
                                     'export_to_titan',
                                     'graphx_connected_components',
                                     'graphx_pagerank',
                                     'graphx_triangle_count',
                                     'ml',
                                     'name',
                                     'status',
                                     'vertex_count',
                                     'vertices'], atk.Graph)

    def test_expected_methods_exist_on_titangraph(self):
        self.assert_methods_defined(['annotate_degrees',
                                     'annotate_weighted_degrees',
                                     'append',
                                     'clustering_coefficient',
                                     'copy',
                                     'export_to_graph',
                                     'graphx_connected_components',
                                     'graphx_pagerank',
                                     'graphx_triangle_count',
                                     'graph_clustering',
                                     'load',
                                     'ml',
                                     'name',
                                     'query',
                                     'status',
                                     'vertex_sample'], atk.TitanGraph)

    def test_expected_methods_exist_on_titangraphml(self):
        self.assert_methods_defined(['belief_propagation'], atk.core.api.TitanGraphMl)

    def test_expected_methods_exist_on_kmeans_model(self):
        self.assert_methods_defined(["name",
                                     "predict",
                                     "train"], atk.KMeansModel)
    def test_expected_methods_exist_on_lda_model(self):
        self.assert_methods_defined(["name",
                                    "train"], atk.LdaModel)
    def test_expected_methods_exist_on_collaborative_filtering_model(self):
        self.assert_methods_defined(["name",
                                    "train",
                                    "recommend"], atk.CollaborativeFilteringModel)

    def test_expected_methods_exist_on_libsvm_model(self):
        self.assert_methods_defined(["name",
                                     "predict",
                                     "score",
                                     "test",
                                     "train"], atk.LibsvmModel)

    def test_expected_methods_exist_on_linear_regression_model(self):
        self.assert_methods_defined(["name",
                                     "predict",
                                     "train"], atk.LinearRegressionModel)

    def test_expected_methods_exist_on_logistic_regression_model(self):
        self.assert_methods_defined(["name",
                                     "predict",
                                     "test",
                                     "train"], atk.LogisticRegressionModel)

    def test_expected_methods_exist_on_svm_model(self):
        self.assert_methods_defined(["name",
                                     "predict",
                                     "test",
                                     "train"], atk.SvmModel)

    def test_expected_global_methods_exist(self):
        self.assert_methods_defined(['drop_frames',
                                     'drop_graphs',
                                     'drop_models',
                                     'get_frame',
                                     'get_frame_names',
                                     'get_graph',
                                     'get_graph_names',
                                     'get_model',
                                     'get_model_names'], atk)

    def assert_methods_defined(self, methods, clazz):
        for method in methods:
            self.assertTrue(method in dir(clazz), "method " + method + " didn't exist on " + str(clazz))


if __name__ == "__main__":
    unittest.main()
