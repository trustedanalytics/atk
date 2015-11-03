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

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex }
import org.apache.spark.rdd.RDD
import java.io.Serializable
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * This is the main clustering class.
 * @param dbConnectionConfig serializable configuration file
 */
class GraphClusteringWorker(dbConnectionConfig: SerializableBaseConfiguration) extends Serializable with EventLogging {

  private val graphClusteringReport = new StringBuilder

  /**
   * Convert the storage graph into a graph edge RDD
   * @param vertices the list of vertices for the initial graph
   * @param edges the list of edges for the initial graph
   */
  def execute(vertices: RDD[GBVertex], edges: RDD[GBEdge], edgeDistanceProperty: String): String = {

    val clusteringFactory: GraphClusteringStorageFactoryInterface = GraphClusteringStorageFactory(dbConnectionConfig)
    val hcRdd: RDD[GraphClusteringEdge] = edges.map {
      case edge =>
        val edgeDistProperty = edge.getProperty(edgeDistanceProperty)
          .getOrElse(throw new Exception(s"Edge does not have $edgeDistanceProperty property"))

        GraphClusteringEdge(edge.headPhysicalId.asInstanceOf[Number].longValue,
          GraphClusteringConstants.DefaultNodeCount,
          edge.tailPhysicalId.asInstanceOf[Number].longValue,
          GraphClusteringConstants.DefaultNodeCount,
          1 - edgeDistProperty.value.asInstanceOf[Float], isInternal = false)
    }.distinct()

    configStorage(clusteringFactory)
    clusterGraph(hcRdd, clusteringFactory)
    graphClusteringReport.toString()
  }

  /**
   * This is the main loop of the algorithm
   * @param graph initial in memory graph as RDD of graph clustering edges
   */
  def clusterGraph(graph: RDD[GraphClusteringEdge],
                   hcFactory: GraphClusteringStorageFactoryInterface): String = {

    var currentGraph: RDD[GraphClusteringEdge] = graph
    var iteration = 0

    while (currentGraph != null) {
      iteration = iteration + 1
      currentGraph = clusterNewLayer(currentGraph, iteration, hcFactory)
    }
    graphClusteringReport.toString()
  }

  /**
   * Add schema to the storage configuration
   */
  private def configStorage(hcFactory: GraphClusteringStorageFactoryInterface): Unit = {

    val storage = hcFactory.newStorage()
    storage.addSchema()
    storage.shutdown()
  }

  /**
   * Creates a set of meta-node and a set of internal nodes and edges (saved to storage)
   * @param graph (n-1) in memory graph (as an RDD of clustering edges)
   * @param iteration current iteration, testing purposes only
   * @return (n) in memory graph (as an RDD of graph clustering edges)
   */
  private def clusterNewLayer(graph: RDD[GraphClusteringEdge],
                              iteration: Int,
                              hcFactory: GraphClusteringStorageFactoryInterface): RDD[GraphClusteringEdge] = {

    // the list of edges to be collapsed and removed from the active graph
    val collapsableEdges = createCollapsableEdges(graph)
    collapsableEdges.persist(StorageLevel.MEMORY_AND_DISK)

    // the list of internal nodes connecting a newly created meta-node and the nodes of the collapsed edge
    val (internalEdges, nonSelectedEdges) = createInternalEdges(collapsableEdges, iteration, hcFactory)
    internalEdges.persist(StorageLevel.MEMORY_AND_DISK)
    nonSelectedEdges.persist(StorageLevel.MEMORY_AND_DISK)

    // the list of newly created active edges in the graph
    val activeEdges = createActiveEdges(nonSelectedEdges, internalEdges)
    activeEdges.persist(StorageLevel.MEMORY_AND_DISK)

    val iterationCountLog = GraphClusteringConstants.IterationMarker + " " + iteration
    graphClusteringReport.append(iterationCountLog + "\n")
    info(iterationCountLog)

    val collapsableEdgesCount = collapsableEdges.count()
    if (collapsableEdges.count() > 0) {
      val log = "Collapsed edges " + collapsableEdgesCount
      graphClusteringReport.append(log + "\n")
      info(log)
    }
    else {
      val log = "No new collapsed edges"
      graphClusteringReport.append(log + "\n")
      info(log)
    }

    val internalEdgesCount = internalEdges.count()
    if (internalEdgesCount > 0) {
      val log = "Internal edges " + internalEdgesCount
      graphClusteringReport.append(log + "\n")
      info(log)
    }
    else {
      val log = "No new internal edges"
      graphClusteringReport.append(log + "\n")
      info(log)
    }

    val activeEdgesCount = activeEdges.count()
    if (activeEdges.count > 0) {
      internalEdges.unpersist()

      val activeEdgesLog = "Active edges " + activeEdgesCount
      graphClusteringReport.append(activeEdgesLog + "\n")
      info(activeEdgesLog)

      // create a key-value pair list of edges from the current graph (for subtractByKey)
      val currentGraphAsKVPair = graph.map((e: GraphClusteringEdge) => (e.src, e))

      // create a key-value pair list of edges from the list of edges to be collapsed for subtractByKey)
      val collapsedEdgesAsKVPair = collapsableEdges.flatMap {
        case (collapsedEdge, nonSelectedEdges) => Seq((collapsedEdge.src, null),
          (collapsedEdge.dest, null))
      }

      //remove collapsed edges from the active graph - by src node
      val newGraphReducedBySrc = currentGraphAsKVPair.subtractByKey(collapsedEdgesAsKVPair).values

      //double the edges for edge selection algorithm
      val activeEdgesBothDirections = activeEdges.flatMap((e: GraphClusteringEdge) => Seq(e, GraphClusteringEdge(e.dest,
        e.destNodeCount,
        e.src,
        e.srcNodeCount,
        e.distance, e.isInternal))).distinct()
      activeEdges.unpersist()
      nonSelectedEdges.unpersist()

      //remove collapsed edges from the active graph - by dest node
      val newGraphReducedBySrcAndDest = newGraphReducedBySrc.map((e: GraphClusteringEdge) => (e.dest, e)).subtractByKey(collapsedEdgesAsKVPair).values
      val newGraphWithoutInternalEdges = activeEdgesBothDirections.union(newGraphReducedBySrcAndDest).coalesce(activeEdgesBothDirections.partitions.length, shuffle = true)
      val distinctNewGraphWithoutInternalEdges = newGraphWithoutInternalEdges.filter(e => e.src != e.dest)

      collapsableEdges.unpersist()

      val nextItLog = "Active edges to next iteration " + distinctNewGraphWithoutInternalEdges.count()
      graphClusteringReport.append(nextItLog + "\n")
      info(nextItLog)

      distinctNewGraphWithoutInternalEdges
    }
    else {
      val log = "No new active edges - terminating..."
      graphClusteringReport.append(log + "\n")
      info(log)

      null
    }

  }

  /**
   * Create a set of edges to be added to the graph, replacing the collapsed ones
   * @param nonSelectedEdges - the set of collapsed edges
   * @param internalEdges - the set of internal edges (previously calculated from collapsed ones)
   * @return a list of new edges (containing meta-nodes) to be added to the active graph. The edge distance is updated/calculated for the new edges
   */
  private def createActiveEdges(nonSelectedEdges: RDD[GraphClusteringEdge],
                                internalEdges: RDD[GraphClusteringEdge]): RDD[GraphClusteringEdge] = {

    val activeEdges = nonSelectedEdges.map {
      case (e) => ((e.src, e.dest, e.destNodeCount), e)
    }.groupByKey()

    // create new active edges
    val activeEdgesWithWeightedAvgDistance = activeEdges.map {
      case ((srcNode, destNode, destNodeCount), newEdges) =>
        val tempEdgeForMetaNode = newEdges.head

        GraphClusteringEdge(tempEdgeForMetaNode.src,
          tempEdgeForMetaNode.srcNodeCount,
          destNode,
          destNodeCount,
          EdgeDistance.weightedAvg(newEdges), isInternal = false)
    }.distinct()

    val newEdges = (internalEdges union activeEdgesWithWeightedAvgDistance).coalesce(internalEdges.partitions.length, shuffle = true).map(
      (e: GraphClusteringEdge) => (e.dest, e)
    ).groupByKey()

    // update the dest node with meta-node in the list
    val newEdgesWithMetaNodeForDest = newEdges.map {
      case (dest, newEdges) => EdgeManager.replaceWithMetaNode(newEdges)
    }.flatMap(identity)

    val newEdgesWithMetaNodeGrouped = newEdgesWithMetaNodeForDest.map(
      (e: GraphClusteringEdge) => ((e.src, e.dest), e)
    ).groupByKey()

    // recalculate the edge distance if several outgoing edges go into the same meta-node
    val newEdgesWithMetaNodesAndDistUpdated = newEdgesWithMetaNodeGrouped.map {
      case ((src, dest), edges) => EdgeDistance.simpleAvg(edges, swapInfo = true)
    }.map {
      (e: GraphClusteringEdge) => ((e.src, e.dest), e)
    }.groupByKey()

    newEdgesWithMetaNodesAndDistUpdated.map {
      case ((src, dest), edges) => EdgeDistance.simpleAvg(edges, swapInfo = false)
    }
  }

  /**
   * Create internal edges for all collapsed edges of the graph
   * @param collapsedEdges a list of edges to be collapsed
   * @return 2 RDDs - one with internal edges and a second with non-minimal distance edges. The RDDs will be used
   *         to calculate the new active edges for the current iteration.
   */
  private def createInternalEdges(collapsedEdges: RDD[(GraphClusteringEdge, Iterable[GraphClusteringEdge])],
                                  iteration: Int,
                                  hcFactory: GraphClusteringStorageFactoryInterface): (RDD[GraphClusteringEdge], RDD[GraphClusteringEdge]) = {

    val internalEdges = collapsedEdges.mapPartitions {
      case edges: Iterator[(GraphClusteringEdge, Iterable[GraphClusteringEdge])] => {
        val hcStorage = hcFactory.newStorage()

        val result = edges.map {
          case (minDistEdge, nonMinDistEdges) =>
            val (metanode, metanodeCount, metaEdges) = EdgeManager.createInternalEdgesForMetaNode(minDistEdge, hcStorage, iteration)
            val replacedEdges = EdgeManager.createActiveEdgesForMetaNode(metanode, metanodeCount, nonMinDistEdges).map(_._2)
            (metaEdges, replacedEdges)
        }.toList

        hcStorage.commit()
        hcStorage.shutdown()

        result.toIterator
      }
    }
    internalEdges.persist(StorageLevel.MEMORY_AND_DISK)
    (internalEdges.flatMap(_._1), internalEdges.flatMap(_._2))
  }

  /**
   * Create collapsed edges for the current graph
   * @param graph the active graph at ith iteration
   * @return a list of edges to be collapsed at this iteration
   */
  private def createCollapsableEdges(graph: RDD[GraphClusteringEdge]): RDD[(GraphClusteringEdge, Iterable[GraphClusteringEdge])] = {

    val edgesBySourceIdWithMinEdge = graph.map((e: GraphClusteringEdge) => (e.src, e)).groupByKey().map {
      case (minEdge, allEdges) => EdgeDistance.min(allEdges)
    }.groupByKey().filter {
      case (minEdge,
        pairedEdgeList: Iterable[VertexOutEdges]) => EdgeManager.canEdgeCollapse(pairedEdgeList)
    }

    edgesBySourceIdWithMinEdge.map {
      case (minEdge, pairedEdgeList: Iterable[VertexOutEdges]) =>
        EdgeManager.createOutgoingEdgesForMetaNode(pairedEdgeList)
    }.filter {
      case (collapsableEdge, outgoingEdgeList) => collapsableEdge != null
    }
  }
}
