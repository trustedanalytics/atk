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

import org.trustedanalytics.atk.domain.DomainJsonProtocol
import org.trustedanalytics.atk.domain.graph.SeamlessGraphMeta
import org.trustedanalytics.atk.engine.graph.{ SparkEdgeFrame, SparkVertexFrame, SparkGraph }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import scala.collection.immutable.Map
import spray.json._

/** Json conversion for arguments and return value case classes */
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object ExportOrientDbGraphJsonFormat {
  implicit val exportOrientDbGraphArgsFormat = jsonFormat3(ExportOrientDbGraphArgs)
  implicit val StatisticsFormat = jsonFormat2(Statistics)
  implicit val exportOrientDbGraphReturnFormat = jsonFormat3(ExportOrientDbGraphReturn)
}
import org.trustedanalytics.atk.plugins.orientdb.ExportOrientDbGraphJsonFormat._

@PluginDoc(oneLine = "Exports graph to OrientDB",
  extended =
    """Creates OrientDB database using the parameters provided in the configurations file.

      OrientDB database will be located according to the given host name and port number,
      and with the given user credentials after checking the user authorization to create or access OrientDB database.

      Then exports the graph edges and vertices schemas,
      exports the vertices and finally, the edges by looking up the source vertex
      and destination vertex using the vertex IDs and creates the edge.""",
  returns =
    """the location to the OrientDB database file "URI", in addition to dictionary for the exported vertices and edges.
      for vertices dictionary:
             it returns the vertex class name, the number of the exported vertices and the number of vertices failed to be exported.
      for the edges dictionary:
             it returns the edge class name, the number of the exported edges and the number of edges that failed to be exported.""")
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
   * @return a value of type declared as the return type.
   */

  def exportVertexFramesToOrient(arguments: ExportOrientDbGraphArgs, dbConfigurations: DbConfigurations, graphMeta: SeamlessGraphMeta)(implicit invocation: Invocation): Map[String, Statistics] = {

    val orientDatabase = GraphDbFactory.graphDbConnector(dbConfigurations)
    val vertexFrames = graphMeta.vertexFrames.map(_.toReference)
    val metadata = vertexFrames.map(frame => {
      val sparkFrame: SparkVertexFrame = frame
      val vertexFrameRdd = sparkFrame.rdd
      val vertexSchema = vertexFrameRdd.vertexWrapper.schema
      if (orientDatabase.getVertexType(vertexSchema.label) == null) {
        val schemaWriter = new SchemaWriter(orientDatabase)
        val oVertexType = schemaWriter.createVertexSchema(vertexSchema)
      }
      val exportVertexFrame = new VertexFrameWriter(vertexFrameRdd, dbConfigurations)
      val verticesCount = exportVertexFrame.exportVertexFrame(arguments.batchSize)
      val exportedVerticesCount = orientDatabase.countVertices(vertexSchema.label)
      val failedVerticesCount = verticesCount - exportedVerticesCount
      (vertexSchema.label, Statistics(exportedVerticesCount, failedVerticesCount))
    })
    metadata.toMap
  }

  /**
   * A method for exporting the edge frames to OrientDB edges
   *
   * @param arguments the arguments supplied by the caller
   * @param graphMeta the graph metadata
   * @param invocation
   * @return a value of type declared as the return type.
   */

  def exportEdgeFramesToOrient(arguments: ExportOrientDbGraphArgs, dbConfigurations: DbConfigurations, graphMeta: SeamlessGraphMeta)(implicit invocation: Invocation): Map[String, Statistics] = {

    val orientDatabase = GraphDbFactory.graphDbConnector(dbConfigurations)
    val edgeFrames = graphMeta.edgeFrames.map(_.toReference)
    val metadata = edgeFrames.map(frame => {
      val sparkFrame: SparkEdgeFrame = frame
      val edgeFrameRdd = sparkFrame.rdd
      val edgeSchema = edgeFrameRdd.edge.schema
      if (orientDatabase.getEdgeType(edgeSchema.label) == null) {
        val schemaWriter = new SchemaWriter(orientDatabase)
        val edgeType = schemaWriter.createEdgeSchema(edgeSchema)
      }
      val exportEdgeFrame = new EdgeFrameWriter(edgeFrameRdd, dbConfigurations)
      val edgesCount = exportEdgeFrame.exportEdgeFrame(arguments.batchSize)
      val exportedEdgesCount = orientDatabase.countEdges(edgeSchema.label)
      val failedEdgesCount = edgesCount - exportedEdgesCount
      (edgeSchema.label, Statistics(exportedEdgesCount, failedEdgesCount))
    })
    metadata.toMap
  }
}