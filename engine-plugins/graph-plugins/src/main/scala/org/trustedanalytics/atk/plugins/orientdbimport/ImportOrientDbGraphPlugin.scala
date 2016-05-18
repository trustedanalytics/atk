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
package org.trustedanalytics.atk.plugins.orientdbimport
import org.apache.spark.atk.graph.{ EdgeFrameRdd, VertexFrameRdd }
import org.trustedanalytics.atk.domain.graph.GraphEntity
import org.trustedanalytics.atk.domain.schema.{ EdgeSchema, VertexSchema }
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ Invocation, SparkCommandPlugin, PluginDoc }
import org.trustedanalytics.atk.plugins.orientdb.DbConfigReader
import spray.json._

import scala.collection.mutable.ArrayBuffer

/** Json conversion for arguments and return value case classes */
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object importOrientDbGraphJsonFormat {
  implicit val importOrientDbGraphArgsFormat = jsonFormat2(ImportOrientDbGraphArgs)
  // implicit val importOrientDbGraphReturnFormat = jsonFormat3(ImportOrientDbGraphReturn)
}
import org.trustedanalytics.atk.plugins.orientdbimport.importOrientDbGraphJsonFormat._

@PluginDoc(oneLine = "Imports a graph from OrientDB database to ATK",
  extended = ".",
  returns = "ATK graph in Parquet graph format.")
class ImportOrientDbGraphPlugin extends SparkCommandPlugin[ImportOrientDbGraphArgs, GraphEntity] {

  override def name: String = "graph:/_import_orientdb"

  /**
   * A method imports OrientDB graph to ATK Parquet graph format
   *
   * @param arguments the arguments supplied by the caller
   * @param invocation
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ImportOrientDbGraphArgs)(implicit invocation: Invocation): GraphEntity = {

    //Get the graph meta data
    val graph: SparkGraph = arguments.graph
    // Get OrientDB configurations
    val dbConfig = DbConfigReader.extractConfigurations(arguments.graphName)
    val loadVertexFrame = new LoadVertexFrame(dbConfig)
    val vertexFrameRdds = loadVertexFrame.importOrientDbVertexClass(sc)
    val loadEdgeFrame = new LoadEdgeFrame(dbConfig)
    val edgeFrameRdds = loadEdgeFrame.importOrientDbEdgeClass(sc)

    saveVertexFrames(graph, vertexFrameRdds)

    saveEdgeFrames(graph, edgeFrameRdds)
    graph
  }

  /**
   *
   * @param graph
   * @param edgeFrameRdds
   */
  def saveEdgeFrames(graph: SparkGraph, edgeFrameRdds: List[EdgeFrameRdd])(implicit invocation: Invocation): Unit = {
    edgeFrameRdds.foreach(edgeFrameRdd => {
      val edgeFrameSchema = edgeFrameRdd.frameSchema.asInstanceOf[EdgeSchema]
      graph.defineEdgeType(edgeFrameRdd.frameSchema.asInstanceOf[EdgeSchema])
      //Get the list of the graph from the meta data
      val graphMeta = engine.graphs.expectSeamless(graph)
      val edgeFrame = graphMeta.edgeMeta(edgeFrameSchema.label)
      engine.graphs.saveEdgeRdd(edgeFrame.toReference, edgeFrameRdd)
    })
  }

  /**
   *
   * @param graph
   * @param vertexFrameRdds
   */
  def saveVertexFrames(graph: SparkGraph, vertexFrameRdds: List[VertexFrameRdd])(implicit invocation: Invocation): Unit = {
    vertexFrameRdds.foreach(vertexFrameRdd => {
      val vertexFrameSchema = vertexFrameRdd.frameSchema.asInstanceOf[VertexSchema]
      graph.defineVertexType(vertexFrameRdd.frameSchema.asInstanceOf[VertexSchema])
      //Get the list of the graph from the meta data
      val graphMeta = engine.graphs.expectSeamless(graph)
      val vertexFrame = graphMeta.vertexMeta(vertexFrameSchema.label)
      engine.graphs.saveVertexRdd(vertexFrame.toReference, vertexFrameRdd)
    })
  }
}