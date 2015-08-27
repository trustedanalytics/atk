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

package org.trustedanalytics.atk.plugins.exportfromtitan

import org.trustedanalytics.atk.domain.StorageFormats
import org.trustedanalytics.atk.engine.graph.plugins.exportfromtitan.{ VertexSchemaAggregator, EdgeSchemaAggregator, EdgeHolder }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.trustedanalytics.atk.domain.graph._
import org.trustedanalytics.atk.domain.schema.DataTypes._
import org.trustedanalytics.atk.domain.schema.{ Column, VertexSchema, _ }
import org.trustedanalytics.atk.engine.{ FrameStorage, GraphStorage }
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrameStorage
import org.trustedanalytics.atk.engine.graph.{ SparkGraph, SparkGraphStorage }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import com.tinkerpop.blueprints.Vertex
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.atk.graph.{ VertexWrapper, EdgeWrapper }
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext._

import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._

/**
 * holds 3 labels
 * @param edgeLabel the label of the edge
 * @param srcLabel the label (or type) of the source vertex
 * @param destLabel the label (or type) of the destination vertex
 */
case class LabelTriplet(edgeLabel: String, srcLabel: String, destLabel: String)

@PluginDoc(oneLine = "Export from ta.TitanGraph to ta.Graph.",
  extended = "")
class ExportToGraphPlugin extends SparkCommandPlugin[GraphNoArgs, GraphEntity] {

  override def name: String = "graph:titan/export_to_graph"

  override def numberOfJobs(arguments: GraphNoArgs)(implicit invocation: Invocation): Int = {
    // NOTE: there isn't really a good way to set this number for this plugin
    8
  }

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: GraphNoArgs)(implicit invocation: Invocation): GraphEntity = {

    val graph: SparkGraph = arguments.graph
    require(graph.isTitan, "Titan graph is required for this operation")

    val (vertices, edges) = graph.gbRdds
    vertices.cache()
    edges.cache()

    // We're going to re-use the physical ids that were present in Titan for _vid and _eid
    val maxVertexId = vertices.map(v => v.physicalId.asInstanceOf[Long]).reduce((a, b) => Math.max(a, b))
    val maxEdgeId = edges.flatMap(e => e.eid).reduce((a, b) => Math.max(a, b))

    // unique indices should exist on Vertex properties that were a user-defined Vertex ID
    val indexNames: List[String] = ExportToGraphPlugin.uniqueVertexPropertyIndices(engine.graphs, graph)

    // Label all of the vertices
    val labeledVertices = vertices.labelVertices(indexNames)

    val edgesWithCorrectedLabels = correctEdgeLabels(labeledVertices, edges)

    // Use Spark aggregation to figure out all of the Vertex and Edge schemas
    val vertexSchemaAggregator = new VertexSchemaAggregator(indexNames)
    val vertexSchemas = labeledVertices.aggregate(vertexSchemaAggregator.zeroValue)(vertexSchemaAggregator.seqOp, vertexSchemaAggregator.combOp).values
    val edgeSchemas = edgesWithCorrectedLabels.aggregate(EdgeSchemaAggregator.zeroValue)(EdgeSchemaAggregator.seqOp, EdgeSchemaAggregator.combOp).values

    // Create the target Graph
    val targetGraph: SparkGraph = engine.graphs.createGraph(GraphTemplate(None, StorageFormats.SeamlessGraph))

    // Create the Edge and Vertex frames
    vertexSchemas.foreach(schema => targetGraph.defineVertexType(schema))
    edgeSchemas.foreach(schema => targetGraph.defineEdgeType(schema))

    saveVertices(engine.graphs, engine.frames.asInstanceOf[SparkFrameStorage], labeledVertices, targetGraph.toReference)
    saveEdges(engine.graphs, engine.frames, edgesWithCorrectedLabels.map(_.edge), targetGraph.toReference)

    vertices.unpersist()
    edges.unpersist()

    targetGraph.incrementIdCounter(Math.max(maxVertexId, maxEdgeId) + 1)
    targetGraph
  }

  /**
   * Edges in "Seamless Graph" have a source vertex label and destination vertex label.
   * This is more restrictive than Titan so some edges from Titan might need to be re-labeled.
   *
   * For example, Titan may have 'ratings' edges that are directed from 'users' to 'movies' and
   * others that are directed from 'movies' to 'users'.  When these edges are put in Seamless graph
   * there will need to be two edge types, one per direction.
   *
   * @param labeledVertices vertices with an "_label" property
   * @param edges  edges with labels as they came out of Titan
   * @return edges with corrected labels (modified if needed)
   */
  def correctEdgeLabels(labeledVertices: RDD[GBVertex], edges: RDD[GBEdge]): RDD[EdgeHolder] = {

    // Join edges with vertex labels so that we can find the source and target for each edge type
    val vidsAndLabels = labeledVertices.map(vertex => (vertex.physicalId.asInstanceOf[Long], vertex.getProperty(GraphSchema.labelProperty).get.value.toString))
    val edgesByHead = edges.map(edge => (edge.headPhysicalId.asInstanceOf[Long], EdgeHolder(edge, null, null)))
    val edgesWithHeadLabels = edgesByHead.join(vidsAndLabels).values.map(pair => pair._1.copy(srcLabel = pair._2))
    val joined = edgesWithHeadLabels.map(e => (e.edge.tailPhysicalId.asInstanceOf[Long], e)).join(vidsAndLabels).values.map(pair => pair._1.copy(destLabel = pair._2))
    joined.cache()

    // Edges in "Seamless Graph" have a source vertex label and destination vertex label.
    // This is more restrictive than Titan so some edges from Titan might need to be re-labeled
    val labels = joined.map(e => LabelTriplet(e.edge.label, e.srcLabel, e.destLabel)).distinct().collect()
    val edgeLabelsMap = ExportToGraphPlugin.buildTargetMapping(labels)
    val edgesWithCorrectedLabels = joined.map(edgeHolder => {
      val targetLabel = edgeLabelsMap.get(LabelTriplet(edgeHolder.edge.label, edgeHolder.srcLabel, edgeHolder.destLabel))
      if (targetLabel.isEmpty) {
        throw new RuntimeException(s"targetLabel wasn't found in map $edgeLabelsMap")
      }
      EdgeHolder(edgeHolder.edge.copy(label = targetLabel.get), edgeHolder.srcLabel, edgeHolder.destLabel)
    })

    joined.unpersist()

    edgesWithCorrectedLabels
  }

  /**
   * Save the edges
   * @param edges edge rdd
   * @param targetGraph destination graph instance
   */
  def saveEdges(graphs: GraphStorage, frames: SparkFrameStorage, edges: RDD[GBEdge], targetGraph: GraphReference)(implicit invocation: Invocation) {
    graphs.expectSeamless(targetGraph).edgeFrames.foreach(edgeFrame => {
      val schema = edgeFrame.schema.asInstanceOf[EdgeSchema]
      val filteredEdges: RDD[GBEdge] = edges.filter(e => e.label == schema.label)

      val edgeWrapper = new EdgeWrapper(schema)
      val rows = filteredEdges.map(gbEdge => edgeWrapper.create(gbEdge))
      val edgeFrameRdd = new FrameRdd(schema, rows)
      frames.saveFrameData(edgeFrame.toReference, edgeFrameRdd)
    })
  }

  /**
   * Save the vertices
   * @param vertices vertices rdd
   * @param targetGraph destination graph instance
   */
  def saveVertices(graphs: GraphStorage, frames: SparkFrameStorage, vertices: RDD[GBVertex], targetGraph: GraphReference)(implicit invocation: Invocation) {
    graphs.expectSeamless(targetGraph).vertexFrames.foreach(vertexFrame => {
      val schema = vertexFrame.schema.asInstanceOf[VertexSchema]

      val filteredVertices: RDD[GBVertex] = vertices.filter(v => {
        v.getProperty(GraphSchema.labelProperty) match {
          case Some(p) => p.value == schema.label
          case _ => throw new RuntimeException(s"Vertex didn't have a label property $v")
        }
      })

      val vertexWrapper = new VertexWrapper(schema)
      val rows = filteredVertices.map(gbVertex => vertexWrapper.create(gbVertex))
      val vertexFrameRdd = new FrameRdd(schema, rows)
      frames.saveFrameData(vertexFrame.toReference, vertexFrameRdd)
    })
  }
}

object ExportToGraphPlugin {

  /**
   * Unique indices should exist on Vertex properties that were a user-defined Vertex ID
   * @return vertex properties that had 'unique' indices
   */
  def uniqueVertexPropertyIndices(graphs: SparkGraphStorage, titanGraph: GraphReference)(implicit context: Invocation): List[String] = {
    val graph = graphs.titanGraph(titanGraph)
    try {
      val indexes = graph.getManagementSystem.getGraphIndexes(classOf[Vertex])
      val uniqueIndexNames = indexes.iterator().toList.filter(_.isUnique).map(_.getName)
      uniqueIndexNames
    }
    finally {
      graph.shutdown()
    }
  }

  /**
   * Create mapping of Triplets to target labels.
   *
   * Edges in "Seamless Graph" have a source vertex label and destination vertex label.
   * This is more restrictive than Titan so some edges from Titan might need to be re-labeled
   *
   * @param labelTriplets labels as they exist in Titan
   * @return LabelTriplet to the target label that will actually be used during edge output
   */
  def buildTargetMapping(labelTriplets: Array[LabelTriplet]): Map[LabelTriplet, String] = {
    var usedLabels = Set[String]()

    var result = Map[LabelTriplet, String]()
    for (labelTriplet <- labelTriplets) {
      var label = labelTriplet.edgeLabel
      var counter = 1
      while (usedLabels.contains(label)) {
        label = labelTriplet + "_" + counter
        counter = counter + 1
      }
      usedLabels += label
      result += labelTriplet -> labelTriplet.edgeLabel
    }
    result
  }

}
