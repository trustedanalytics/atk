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

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.atk.graph.{ EdgeFrameRdd, VertexFrameRdd }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameName, FrameEntity }
import org.trustedanalytics.atk.domain.graph.GraphEntity
import org.trustedanalytics.atk.domain.schema.{ EdgeSchema, VertexSchema, GraphSchema }
import org.trustedanalytics.atk.engine.graph.{ SparkVertexFrame, SparkGraph }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, SparkCommandPlugin, PluginDoc }
import org.trustedanalytics.atk.plugins.orientdb.{ GraphDbFactory, DbConfigReader }
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
    val vertexFrameRdd: VertexFrameRdd = loadVertexFrame.importOrientDbVertexClass(sc)
    val loadEdgeFrame = new LoadEdgeFrame(dbConfig)
    val edgeFrameRdd = loadEdgeFrame.importOrientDbEdgeClass(sc)
    val vertexFrameSchema = vertexFrameRdd.frameSchema.asInstanceOf[VertexSchema]
    graph.defineVertexType(vertexFrameRdd.frameSchema.asInstanceOf[VertexSchema]) // add schema method to VertexFrameRdd
    val edgeFrameSchema = edgeFrameRdd.frameSchema.asInstanceOf[EdgeSchema]
    graph.defineEdgeType(edgeFrameRdd.frameSchema.asInstanceOf[EdgeSchema])
    //Get the list of the graph from the meta data
    val graphMeta = engine.graphs.expectSeamless(graph)
    val vertexFrame = graphMeta.vertexMeta(vertexFrameSchema.label)
    engine.graphs.saveVertexRdd(vertexFrame.toReference, vertexFrameRdd)
    val edgeFrame = graphMeta.edgeMeta(edgeFrameSchema.label)
    engine.graphs.saveEdgeRdd(edgeFrame.toReference, edgeFrameRdd)
    graph
  }
}