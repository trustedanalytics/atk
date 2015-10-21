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
Results post-processing
"""

# This is a stop-gap solution for post-processing results from commands whose
# definitions are dynamically loaded from the server


def get_postprocessor(command_full_name):
    """
    Look for a result post-processing function.
    Returns None if not found.
    """
    return _postprocessors.get(command_full_name, None)


# command-> post-processor table, populated by decorator
_postprocessors = {}


# decorator
def postprocessor(*command_full_names):
    def _postprocessor(function):
        for name in command_full_names:
            add_postprocessor(name, function)
        return function
    return _postprocessor


def add_postprocessor(command_full_name, function):
    if command_full_name in _postprocessors:
        raise RuntimeError("Internal Error: duplicate command name '%s' in results post-processors" % command_full_name)
    _postprocessors[command_full_name] = function

def add_return_none_postprocessor(command_full_name):
    add_postprocessor(command_full_name, return_none)


# post-processor methods --all take a json object argument

@postprocessor('graph:titan/vertex_sample', 'graph:/export_to_titan', 'graph:titan/export_to_graph',
               'graph:titan/annotate_degrees', 'graph:titan/annotate_weighted_degrees', 'graph/copy')
def return_graph(json_result):

    from trustedanalytics import get_graph
    return get_graph(json_result['uri'])

@postprocessor('frame/classification_metrics', 'model:logistic_regression/test', 'model:svm/test',
               'model:random_forest_classifier/test', "model:libsvm/test")
def return_metrics(json_result):
     from trustedanalytics.core.classifymetrics import ClassificationMetricsResult
     return ClassificationMetricsResult(json_result)

@postprocessor('frame/tally', 'frame/tally_percent', 'frame/cumulative_sum', 'frame/cumulative_percent', 'frame:/drop_columns',
               'frame/bin_column', 'frame/drop_duplicates', 'frame/flatten_column', 'graph:titan/sampling/assign_sample')
def return_none(json_result):
    return None

@postprocessor('frame/histogram')
def return_histogram(json_result):
    from trustedanalytics.core.histogram import Histogram
    return Histogram(json_result["cutoffs"], json_result["hist"], json_result["density"])

@postprocessor('graph/clustering_coefficient')
def return_clustering_coefficient(json_result):
    from trustedanalytics import get_frame
    from trustedanalytics.core.clusteringcoefficient import  ClusteringCoefficient
    if json_result.has_key('frame'):
        frame = get_frame(json_result['frame']['uri'])
    else:
        frame = None
    return ClusteringCoefficient(json_result['global_clustering_coefficient'], frame)

@postprocessor('frame/bin_column_equal_depth', 'frame/bin_column_equal_width')
def return_bin_result(json_result):
    return json_result["cutoffs"]

@postprocessor('model:lda/train')
def return_lda_train(json_result):
    from trustedanalytics import get_frame
    doc_frame = get_frame(json_result['topics_given_doc']['uri'])
    word_frame= get_frame(json_result['word_given_topics']['uri'])
    topic_frame= get_frame(json_result['topics_given_word']['uri'])
    return { 'topics_given_doc': doc_frame, 'word_given_topics': word_frame, 'topics_given_word': topic_frame, 'report': json_result['report'] }

@postprocessor('model:logistic_regression/train')
def return_logistic_regression_train(json_result):
    from trustedanalytics.core.logisticregression import LogisticRegressionSummary
    return LogisticRegressionSummary(json_result)

@postprocessor('frame:/label_propagation')
def return_label_propagation(json_result):
    from trustedanalytics import get_frame
    frame = get_frame(json_result['output_frame']['uri'])
    return { 'frame': frame, 'report': json_result['report'] }

@postprocessor('frame:/loopy_belief_propagation')
def return_loopy_belief_propagation(json_result):
    from trustedanalytics import get_frame
    frame = get_frame(json_result['output_frame']['uri'])
    return { 'frame': frame, 'report': json_result['report'] }

@postprocessor('frame:/collaborative_filtering')
def return_collaborative_filtering(json_result):
    from trustedanalytics import get_frame
    user_frame = get_frame(json_result['user_frame']['uri'])
    item_frame= get_frame(json_result['item_frame']['uri'])
    return { 'user_frame': user_frame, 'item_frame': item_frame, 'report': json_result['report'] }

@postprocessor('graph/graphx_connected_components','graph/annotate_weighted_degrees','graph/annotate_degrees','graph/graphx_triangle_count')
def return_connected_components(json_result):
    from trustedanalytics import get_frame
    dictionary = json_result["frame_dictionary_output"]
    return dict([(k,get_frame(v["uri"])) for k,v in dictionary.items()])

@postprocessor('graph/graphx_pagerank')
def return_page_rank(json_result):
    from trustedanalytics import get_frame
    vertex_json = json_result["vertex_dictionary_output"]
    edge_json = json_result["edge_dictionary_output"]
    vertex_dictionary = dict([(k,get_frame(v["uri"])) for k,v in vertex_json.items()])
    edge_dictionary = dict([(k,get_frame(v["uri"])) for k,v in edge_json.items()])
    return {'vertex_dictionary': vertex_dictionary, 'edge_dictionary': edge_dictionary}

@postprocessor('graph/ml/belief_propagation','graph:/ml/kclique_percolation')
def return_belief_propagation(json_result):
    from trustedanalytics import get_frame
    vertex_json = json_result['frame_dictionary_output']
    vertex_dictionary = dict([(k,get_frame(v["uri"])) for k,v in vertex_json.items()])
    return {'vertex_dictionary': vertex_dictionary, 'time': json_result['time']}

