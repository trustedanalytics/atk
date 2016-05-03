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

import org.trustedanalytics.atk.domain.graph.SeamlessGraphMeta
import org.trustedanalytics.atk.engine.graph.{ SparkEdgeFrame, SparkVertexFrame, SparkGraph }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import scala.collection.immutable.Map
import spray.json._

/** Json conversion for arguments and return value case classes */
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object ExportOrientDbGraphJsonFormat {

  implicit val exportOrientDbGraphArgsFormat = jsonFormat3(ExportOrientDbGraphArgs)
  implicit val exportOrientDbGraphReturnFormat = jsonFormat3(ExportOrientDbGraphReturn)
}
import org.trustedanalytics.atk.plugins.orientdb.ExportOrientDbGraphJsonFormat._

@PluginDoc(oneLine = "Export graph to OrientDB",
  extended = """Exports the graph to OrientDB database located in the given dbURI.""",
  returns = """OrientDB file, statistics for the exported vertices and edges.""")
class ExportOrientDbGraphPlugin extends SparkCommandPlugin[ExportOrientDbGraphArgs, ExportOrientDbGraphReturn] {

  override def name: String = "graph:/export_to_orientdb"

  /**
   * Method to export graph to OrientDB
   *
   * @param arguments the arguments supplied by the caller
   * @param invocation
   * @return some statistics of the exported vertices and edges
   */

  override def execute(arguments: ExportOrientDbGraphArgs)(implicit invocation: Invocation): ExportOrientDbGraphReturn = {

    // Get OrientDB configurations
    val dbConfig = DbConfigReader.extractConfigurations(arguments.dbName)

    //Get the graph meta data
    val graph: SparkGraph = arguments.graph

    //Get the list of the graph from the meta data
    val graphMeta = engine.graphs.expectSeamless(graph)
    val vertices = exportVertexFramesToOrient(arguments, dbConfig, graphMeta)
    val edges = exportEdgeFramesToOrient(arguments, dbConfig, graphMeta)
    ExportOrientDbGraphReturn(vertices, edges, dbConfig.dbUri)
  }
  /**
   * A method for exporting  vertex frames to Orient vertices
   *
   * @param arguments the arguments supplied by the caller
   * @param graphMeta  the graph meta data
   * @param invocation
   * @return a value of type declared as the Return type.
   */

  def exportVertexFramesToOrient(arguments: ExportOrientDbGraphArgs, dbConfigurations: DbConfigurations, graphMeta: SeamlessGraphMeta)(implicit invocation: Invocation): Map[String, Long] = {

    val vertexFrames = graphMeta.vertexFrames.map(_.toReference)
    val metadata = vertexFrames.map(frame => {
      val sparkFrame: SparkVertexFrame = frame
      val vertexFrameRdd = sparkFrame.rdd
      val exportVertexFrame = new VertexFrameWriter(vertexFrameRdd, dbConfigurations)
      val verticesCount = exportVertexFrame.exportVertexFrame(arguments.batchSize)
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

  def exportEdgeFramesToOrient(arguments: ExportOrientDbGraphArgs, dbConfigurations: DbConfigurations, graphMeta: SeamlessGraphMeta)(implicit invocation: Invocation): Map[String, Long] = {

    val edgeFrames = graphMeta.edgeFrames.map(_.toReference)
    val metadata = edgeFrames.map(frame => {
      val sparkFrame: SparkEdgeFrame = frame
      val edgeFrameRdd = sparkFrame.rdd
      val exportEdgeFrame = new EdgeFrameWriter(edgeFrameRdd, dbConfigurations)
      val edgesCount = exportEdgeFrame.exportEdgeFrame(arguments.batchSize)
      (edgeFrameRdd.edge.schema.label, edgesCount)
    })
    metadata.toMap
  }
}