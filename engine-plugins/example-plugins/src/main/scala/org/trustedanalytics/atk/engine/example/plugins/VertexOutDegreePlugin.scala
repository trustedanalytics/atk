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
/* A sample plugin to compute the out-degree for a graph */

package org.trustedanalytics.atk.engine.example.plugins

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.engine.SparkContextFactory
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc, SparkCommandPlugin }

//Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/* To make msg a mandatory parameter, change to msg: String */
case class VertexOutDegreeInput(@ArgDoc("""Handle to the graph to be used.""") graph: GraphReference)

/** Json conversion for arguments and return value case classes */
object VertexOutDegreeFormat {
  implicit val inputFormat = jsonFormat1(VertexOutDegreeInput)
}

import VertexOutDegreeFormat._
/* VertexOutDegreePlugin will compute the out-degree for each vertex in the graph.
   Users will be able to access the out-degree function on a python graph object as:
       out_degree_frame = graph.vertex_outdegree()
       out_degree_frame.inspect()
 */
@PluginDoc(oneLine = "Counts the out-degree of vertices in a graph.",
  extended =
    """
        Extended Summary
        ----------------
        Extended Summary for Plugin goes here ...
    """,
  returns =
    """
        frame
        Frame with vertex out-degree
    """)
class VertexOutDegreePlugin
    extends SparkCommandPlugin[VertexOutDegreeInput, FrameReference] {

  /**
   * The name of the command, e.g. graph/vertex_outdegree
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   *
   * The colon ":" is used to to indicate command destination base classes, default classes or classes of a
   * specific storage type:
   *
   * - graph:titan means command is loaded into class TitanGraph
   * - graph: means command is loaded into class Graph, our default type which will be the Parquet-backed graph
   * - graph would mean command is loaded into class BaseGraph, which applies to all graph classes
   * - frame: and means command is loaded in class Frame.  Example: "frame:/assign_sample"
   * - model:logistic_regression  means command is loaded into class LogisticRegressionModel
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * For example - you may access these through graph.vertex_outdegree()
   *
   * This plugin is loaded into the BaseGraph format and will be available on all supported graph types
   */
  override def name: String = "graph/vertex_outdegree"

  override def execute(arguments: VertexOutDegreeInput)(implicit invocation: Invocation): FrameReference = {

    /* Load the Frame from the frame id */
    val graph: SparkGraph = arguments.graph

    /* Load the vertex and edge RDDs */
    val (gbVertexRdd, gbEdgeRdd) = graph.gbRdds

    /* Pair vertices by the vertex physical Id*/
    val gbVertexPairRdd = gbVertexRdd.keyBy(vertex => vertex.physicalId)

    /* Pair edges by the the unique Physical ID for the source Vertex (tailPhysicalId) */
    val gbEdgePairRdd = gbEdgeRdd.keyBy(edge => edge.tailPhysicalId)

    /* Count the number of outgoing edges for each vertex */
    val outDegreeRdd = gbVertexPairRdd.join(gbEdgePairRdd).map {
      case (vertexId, (gbVertex, gbEdge)) =>
        (vertexId.toString, 1L)
    }.reduceByKey(_ + _).sortBy(x => x._2, ascending = false)

    /* Convert the out-degree RDD to a Spark row RDD */
    val rowRdd: RDD[sql.Row] = outDegreeRdd.map {
      case (vertexId, outDegree) =>
        new GenericRow(Array[Any](vertexId, outDegree))
    }

    /* Save the vertex output to a frame */
    val outputFrameSchema = FrameSchema(List(Column("vertex_id", DataTypes.str), Column("out_degree", DataTypes.int64)))
    val outputFrameRdd = new FrameRdd(outputFrameSchema, rowRdd)

    /* Register frame with metastore, and save frame */
    val outputFrame = engine.frames.tryNewFrame(CreateEntityArgs(
      description = Some("out-degree frame created by VertexOutDegreePlugin"))) {
      newTrainFrame: FrameEntity =>
        newTrainFrame.save(outputFrameRdd)
    }
    outputFrame
  }
}

