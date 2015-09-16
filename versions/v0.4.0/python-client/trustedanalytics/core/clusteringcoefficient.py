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
Post Processing of clustering_coefficient result
"""
class ClusteringCoefficient:
    """
    Class containing the results of a clustering coefficient operation.

     Members
     --------
     global_clustering_coefficient : Double
        cutoff points of each bin. There are n-1 bins in the result
     frame : Frame
        A Frame is only returned if ``output_property_name`` is provided.
        The frame contains data from every vertex of the graph with its
        local clustering coefficient stored in the user-specified property.
    """
    def __init__(self, global_clustering_coefficient, frame):
        self.global_clustering_coefficient = global_clustering_coefficient
        self.frame = frame

    def __repr__(self):
        return """ClusteringCoefficient:
global_clustering_coefficient: %s,
frame: %s""" % (self.global_clustering_coefficient, self.frame)
