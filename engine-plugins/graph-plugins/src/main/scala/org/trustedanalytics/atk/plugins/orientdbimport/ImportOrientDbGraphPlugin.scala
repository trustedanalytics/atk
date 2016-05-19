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

/** Json conversion for input arguments case class */
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object importOrientDbGraphJsonFormat {
  implicit val importOrientDbGraphArgsFormat = jsonFormat2(ImportOrientDbGraphArgs)
}
import org.trustedanalytics.atk.plugins.orientdbimport.importOrientDbGraphJsonFormat._

@PluginDoc(oneLine = "Imports a graph from OrientDB database to ATK graph",
  extended =
    """Connects to the given OrientDB database name using the settings provided in the configurations file.
       Each imported vertex or edge class from the OrientDB database corresponds to vertex frame RDD or edge frame RDD in ATK.""",
  returns = "ATK graph in Parquet graph format.")
class ImportOrientDbGraphPlugin extends SparkCommandPlugin[ImportOrientDbGraphArgs, GraphEntity] {

  override def name: String = "graph:/_import_orientdb"

  /**
   * A method imports OrientDB graph to ATK Parquet graph format
   */
  override def execute(arguments: ImportOrientDbGraphArgs)(implicit invocation: Invocation): GraphEntity = {

    //Get the graph meta data
    val graph: SparkGraph = arguments.graph
    // Get OrientDB configurations
    val dbConfig = DbConfigReader.extractConfigurations(arguments.graphName)
    val vertexFrameReader = new VertexFrameReader(dbConfig)
    val vertexFrameRdds = vertexFrameReader.importOrientDbVertexClass(sc)
    val edgeFrameReader = new EdgeFrameReader(dbConfig)
    val edgeFrameRdds = edgeFrameReader.importOrientDbEdgeClass(sc)
    saveVertexFrames(graph, vertexFrameRdds)
    saveEdgeFrames(graph, edgeFrameRdds)
    graph
  }

  /**
   * A method saves the edge frame RDDs into the ATK graph
   * @param graph ATK graph
   * @param edgeFrameRdds List of the imported edge frame RDDs
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
   * A method saves the vertex frame RDDs into the ATK graph
   * @param graph ATK graph
   * @param vertexFrameRdds List of the imported vertex frame RDDs
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