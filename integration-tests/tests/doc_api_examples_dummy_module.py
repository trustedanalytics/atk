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
model/rename.rst
frame-vertex/drop_duplicates.rst
graph-/export_to_titan.rst
graph-/ml/kclique_percolation.rst
model-giraph_lda/train.rst
model-giraph_lda/predict.rst
model-giraph_lda/publish.rst
graph-titan/graph_clustering.rst
graph-titan/query/gremlin.rst
graph-titan/query/recommend.rst
graph-titan/sampling/assign_sample.rst
graph-titan/sampling/vertex_sample.rst
frame-/collaborative_filtering.rst
frame-/loopy_belief_propagation.rst
frame-/rename_columns.rst
frame-/label_propagation.rst
graph/graphx_connected_components.rst
graph/graphx_triangle_count.rst
graph/annotate_degrees.rst
graph/graphx_pagerank.rst
graph/clustering_coefficient.rst
graph/annotate_weighted_degrees.rst
graph/ml/kclique_percolation.rst
graph/loopy_belief_propagation.rst
frame/column_median.rst
frame/column_mode.rst
frame/column_summary_statistics.rst
frame/drop_columns.rst
frame/entropy.rst
frame/export_to_csv.rst
frame/export_to_hbase.rst
frame/export_to_hive.rst
frame/export_to_jdbc.rst
frame/export_to_json.rst
frame/rename.rst
""".splitlines())
