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
package org.trustedanalytics.atk.plugins.orientdb

/**
 * Created by wtaie on 3/31/16.
 */

import org.apache.spark.atk.graph.{ EdgeFrameRdd, VertexFrameRdd }
import org.trustedanalytics.atk.domain.graph.SeamlessGraphMeta
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import scala.collection.immutable.Map
import spray.json._

/** Json conversion for arguments and return value case classes */
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object ExportOrientDbGraphJsonFormat {

  implicit val exportOrientDbGraphArgsFormat = jsonFormat5(ExportOrientDbGraphArgs)
  implicit val exportOrientDbGraphReturnFormat = jsonFormat3(ExportOrientDbGraphReturn)
}
import org.trustedanalytics.atk.plugins.orientdb.ExportOrientDbGraphJsonFormat._

@PluginDoc(oneLine = "Export current graph to OrientDb",
  extended = """Export the graph to Orient database file located in the given dbURI.""",
  returns = """OrientDb file, statistics for the exported vertices and edges.""")
class ExportOrientDbGraphPlugin extends SparkCommandPlugin[ExportOrientDbGraphArgs, ExportOrientDbGraphReturn] {

  /**
   *
   * @return
   */
  override def name: String = "graph:/export_to_orientdb"

  /**
   *
   * @param arguments the arguments supplied by the caller
   * @param invocation
   * @return some statistics of the exported vertices and edges
   */

  override def execute(arguments: ExportOrientDbGraphArgs)(implicit invocation: Invocation): ExportOrientDbGraphReturn = {

    //Get the graph meta data
    val graph: SparkGraph = arguments.graph

    //Get the list of the graph from the meta data
    val graphMeta = engine.graphs.expectSeamless(graph)
    val oVerticesStats = exportVertexFramesToOrient(arguments, graphMeta)
    val oEdgesStats = exportEdgeFramesToOrient(arguments, graphMeta)
    ExportOrientDbGraphReturn(oVerticesStats, oEdgesStats, arguments.dbUri)
  }

  /**
   * A method for exporting  vertex frames to Orient vertices
   *
   * @param arguments the arguments supplied by the caller
   * @param graphMeta  the graph meta data
   * @param invocation
   * @return a value of type declared as the Return type.
   */

  def exportVertexFramesToOrient(arguments: ExportOrientDbGraphArgs, graphMeta: SeamlessGraphMeta)(implicit invocation: Invocation): Map[String, Long] = {

    val vertexFrames = graphMeta.vertexFrames.map(_.toReference)
    val metadata = vertexFrames.map(frame => {
      //load frame as RDD
      val sparkFrame: SparkFrame = frame
      val vertexFrameRdd = new VertexFrameRdd(sparkFrame.rdd)
      val exportVertexFrame = new VertexFrameWriter
      val verticesCount = exportVertexFrame.exportVertexFrame(arguments.dbUri, vertexFrameRdd, arguments.batchSize)

      (vertexFrameRdd.vertexWrapper.schema.label, verticesCount)
    })
    metadata.toMap
  }

  /**
   * A method for exporting the edge frames to Orient edges
   *
   * @param arguments the arguments supplied by the caller
   * @param graphMeta the graph meta data
   * @param invocation
   * @return a value of type declared as the Return type.
   */

  def exportEdgeFramesToOrient(arguments: ExportOrientDbGraphArgs, graphMeta: SeamlessGraphMeta)(implicit invocation: Invocation): Map[String, Long] = {

    val edgeFrames = graphMeta.edgeFrames.map(_.toReference)
    val metadata = edgeFrames.map(frame => {
      //load frame as RDD
      val sparkFrame: SparkFrame = frame
      val edgeFrameRdd = new EdgeFrameRdd(sparkFrame.rdd)
      val exportEdgeFrame = new EdgeFrameWriter
      val edgesCount = exportEdgeFrame.exportEdgeFrame(arguments.dbUri, edgeFrameRdd, arguments.batchSize)
      (edgeFrameRdd.edge.schema.label, edgesCount)
    })
    metadata.toMap
  }

}