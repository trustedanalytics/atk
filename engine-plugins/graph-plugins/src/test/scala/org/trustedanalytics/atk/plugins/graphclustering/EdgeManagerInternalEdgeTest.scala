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

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }

class EdgeManagerInternalEdgeTest extends FlatSpec with Matchers with MockitoSugar {

  val mockNodeId = 100
  val minEdge = GraphClusteringEdge(1, 1, 2, 1, 1.1f, false)

  val mockInstance = mock[GraphClusteringStorageInterface]
  when(mockInstance.addVertexAndEdges(1, 2, 2, "1_2", 1)).thenReturn(mockNodeId)

  "edgeManager::createInternalEdgesForMetaNode" should "return default internal edges on null" in {
    val (metanode, metanodeCount, metaEdges) = EdgeManager.createInternalEdgesForMetaNode(null, mockInstance, 1)

    assert(GraphClusteringConstants.DefaultVertextId == metanode)
    assert(GraphClusteringConstants.DefaultNodeCount == metanodeCount)
    assert(metaEdges.isEmpty)
  }

  "edgeManager::createInternalEdgesForMetaNode" should "return valid internal edge list" in {

    val (metanode, metanodeCount, metaEdges) = EdgeManager.createInternalEdgesForMetaNode(minEdge, mockInstance, 1)

    assert(metanode == mockNodeId)
    assert(metanodeCount == 2)
    assert(metaEdges.size == 2)

    metaEdges.foreach(edge =>
      {
        assert(edge.src == mockNodeId)
        assert(edge.srcNodeCount == 2)
        assert(edge.isInternal)
      })
  }
}
