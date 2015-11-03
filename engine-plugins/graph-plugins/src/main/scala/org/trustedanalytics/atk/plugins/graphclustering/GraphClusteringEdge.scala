/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.trustedanalytics.atk.plugins.graphclustering

/**
 * This is the graph clustering edge
 * @param src source node
 * @param srcNodeCount 1 if node is leaf, >1 if meta-node
 * @param dest destination node
 * @param destNodeCount 1 if node is leaf, >1 if meta-node
 * @param distance edge distance
 * @param isInternal true if the edge is internal (created through node edge collapse), false otherwise
 */
case class GraphClusteringEdge(var src: Long,
                               var srcNodeCount: Long,
                               var dest: Long,
                               var destNodeCount: Long,
                               var distance: Float,
                               isInternal: Boolean) {
  /**
   * Get the total node count of the edge
   * @return sum of src + dest counts
   */
  def getTotalNodeCount(): Long = {
    srcNodeCount + destNodeCount
  }

}
