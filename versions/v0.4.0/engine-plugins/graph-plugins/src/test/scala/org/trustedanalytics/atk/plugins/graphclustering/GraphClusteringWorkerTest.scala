/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.plugins.graphclustering

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.scalatest.{ FlatSpec, Matchers }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

case class GraphClusteringStorageMock() extends GraphClusteringStorageInterface {
  val mockNodeId = 100
  var newMetaNodes = 0

  def addSchema(): Unit = {
  }

  def addVertexAndEdges(src: Long, dest: Long, metaNodeCount: Long, metaNodeName: String, iteration: Int): Long = {

    newMetaNodes = newMetaNodes + 1
    mockNodeId + newMetaNodes
  }

  def commit(): Unit = {
  }

  def shutdown(): Unit = {
  }
}

case class GraphClusteringStorageFactoryMock(dbConnectionConfig: SerializableBaseConfiguration)
    extends GraphClusteringStorageFactoryInterface {

  override def newStorage(): GraphClusteringStorageInterface = {

    new GraphClusteringStorageMock
  }
}

class GraphClusteringWorkerTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val emptyEdgeList = List()

  val edgeListOneIteration: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 1, 2, 1, 0.9f, false),
    GraphClusteringEdge(2, 1, 1, 1, 0.9f, false)
  )

  val edgeListTwoIterations: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 1, 2, 1, 0.9f, false),
    GraphClusteringEdge(2, 1, 1, 1, 0.9f, false),
    GraphClusteringEdge(2, 1, 3, 1, 0.8f, false),
    GraphClusteringEdge(3, 1, 2, 1, 0.8f, false)
  )

  val edgeListThreeIterations: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 1, 2, 1, 0.1f, false),
    GraphClusteringEdge(2, 1, 1, 1, 0.1f, false),
    GraphClusteringEdge(2, 1, 3, 1, 0.8f, false),
    GraphClusteringEdge(3, 1, 2, 1, 0.8f, false),
    GraphClusteringEdge(3, 1, 4, 1, 0.7f, false),
    GraphClusteringEdge(4, 1, 3, 1, 0.7f, false),
    GraphClusteringEdge(4, 1, 5, 1, 0.1f, false),
    GraphClusteringEdge(5, 1, 4, 1, 0.1f, false)
  )

  val edgeListFourIterations: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 1, 2, 1, 0.9f, false),
    GraphClusteringEdge(2, 1, 1, 1, 0.9f, false),
    GraphClusteringEdge(2, 1, 3, 1, 0.8f, false),
    GraphClusteringEdge(3, 1, 2, 1, 0.8f, false),
    GraphClusteringEdge(3, 1, 4, 1, 0.7f, false),
    GraphClusteringEdge(4, 1, 3, 1, 0.7f, false),
    GraphClusteringEdge(4, 1, 5, 1, 0.9f, false),
    GraphClusteringEdge(5, 1, 4, 1, 0.9f, false)
  )

  def executeTest(edgeList: List[GraphClusteringEdge], iterationsToComplete: Int): Unit = {
    val worker = new GraphClusteringWorker(null)
    val hcFactoryMock = new GraphClusteringStorageFactoryMock(null)

    val report = worker.clusterGraph(sparkContext.parallelize(edgeList), hcFactoryMock)
    val iterations = GraphClusteringConstants.IterationMarker.r.findAllMatchIn(report).length

    assert(iterations == iterationsToComplete)
  }

  "graphClusteringWorker::mainLoop" should "complete with empty iteration on empty graphs" in {
    executeTest(emptyEdgeList, 1)
  }

  "graphClusteringWorker::mainLoop" should "complete in 1 iteration for connected graphs" in {
    executeTest(edgeListOneIteration, 1)
  }

  "graphClusteringWorker::mainLoop" should "complete in 2 iterations for connected graphs" in {
    executeTest(edgeListTwoIterations, 2)
  }

  "graphClusteringWorker::mainLoop" should "complete in 3 iterations for connected graphs" in {
    executeTest(edgeListThreeIterations, 3)
  }

  "graphClusteringWorker::mainLoop" should "complete in 4 iterations for connected graphs" in {
    executeTest(edgeListFourIterations, 4)
  }
}
