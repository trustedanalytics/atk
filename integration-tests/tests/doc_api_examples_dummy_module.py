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

"""
dummy module used for doctests

The doc_api_examples_tests.py populates the __test__ variable in here and submits it for doctest execution

(also contains the list of doc_api_examples exemptions --.rst file that should be skipped)
"""

import trustedanalytics as ta
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

__test__ = {}


# todo: repair all of the following .rst files to run correctly as doctests and shrink this set to length 0

exemptions = set("""
model-principal_components/train.rst
model-principal_components/predict.rst
model-principal_components/publish.rst
model-libsvm/train.rst
model-libsvm/predict.rst
model-libsvm/test.rst
model-libsvm/score.rst
model/rename.rst
model-random_forest_classifier/predict.rst
model-random_forest_classifier/publish.rst
model-random_forest_classifier/test.rst
model-random_forest_classifier/train.rst
model-random_forest_regressor/predict.rst
model-random_forest_regressor/publish.rst
model-random_forest_regressor/test.rst
model-random_forest_regressor/train.rst
frame-vertex/drop_duplicates.rst
frame-vertex/add_vertices.rst
graph-/vertex_count.rst
graph-/export_to_titan.rst
graph-/_info.rst
graph-/edge_count.rst
graph-/define_edge_type.rst
graph-/define_vertex_type.rst
graph-/ml/kclique_percolation.rst
model-collaborative_filtering/train.rst
model-lda/train.rst
model-lda/predict.rst
model-lda/publish.rst
model-svm/train.rst
model-svm/predict.rst
model-svm/test.rst
model-linear_regression/train.rst
model-linear_regression/predict.rst
model-linear_regression/publish.rst
model-logistic_regression/train.rst
model-logistic_regression/predict.rst
model-logistic_regression/test.rst
graph-titan/graph_clustering.rst
graph-titan/query/gremlin.rst
graph-titan/query/recommend.rst
graph-titan/ml/belief_propagation.rst
graph-titan/sampling/assign_sample.rst
graph-titan/sampling/vertex_sample.rst
frame-/load.rst
frame-/rename_columns.rst
frame-/collaborative_filtering.rst
frame-/loopy_belief_propagation.rst
frame-/filter.rst
(DELETED)frame-/join.rst
frame-/label_propagation.rst
frame-edge/add_edges.rst
graph/graphx_connected_components.rst
graph/graphx_triangle_count.rst
graph/annotate_degrees.rst
graph/graphx_pagerank.rst
graph/copy.rst
graph/clustering_coefficient.rst
graph/annotate_weighted_degrees.rst
graph/ml/kclique_percolation.rst
graph/ml/belief_propagation.rst
model-k_means/train.rst
model-k_means/predict.rst
model-k_means/publish.rst
model-naive_bayes/train.rst
model-naive_bayes/predict.rst
(FIXED)frame/ecdf.rst
frame/export_to_hive.rst
frame/covariance_matrix.rst
frame/cumulative_percent.rst
frame/bin_column_equal_depth.rst
(DELETED)frame/group_by.rst
(FIXED)frame/drop_duplicates.rst
frame/column_summary_statistics.rst
frame/quantiles.rst
frame/export_to_jdbc.rst
frame/histogram.rst
frame/entropy.rst
frame/correlation_matrix.rst
frame/categorical_summary.rst
frame/drop_columns.rst
frame/assign_sample.rst
frame/covariance.rst
frame/flatten_column.rst
frame/copy.rst
frame/count_where.rst
frame/dot_product.rst
frame/export_to_hbase.rst
frame/tally.rst
frame/sort.rst
frame/cumulative_sum.rst
frame/classification_metrics.rst
frame/rename.rst
frame/tally_percent.rst
frame/column_median.rst
frame/unflatten_column.rst
frame/top_k.rst
frame/bin_column.rst
frame/sorted_k.rst
frame/export_to_json.rst
frame/add_columns.rst
frame/correlation.rst
frame/column_mode.rst
frame/export_to_csv.rst
frame/bin_column_equal_width.rst
""".splitlines())
