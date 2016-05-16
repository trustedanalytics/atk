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
import org.apache.spark.atk.graph.{EdgeFrameRdd, VertexFrameRdd}
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.graph.GraphEntity
import org.trustedanalytics.atk.domain.schema.GraphSchema
import org.trustedanalytics.atk.engine.plugin.{Invocation, SparkCommandPlugin, PluginDoc}
import org.trustedanalytics.atk.plugins.orientdb.{GraphDbFactory, DbConfigReader}
import spray.json._

import scala.collection.mutable.ArrayBuffer

/** Json conversion for arguments and return value case classes */
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object importOrientDbGraphJsonFormat {
  implicit val importOrientDbGraphArgsFormat = jsonFormat1(ImportOrientDbGraphArgs)
 // implicit val importOrientDbGraphReturnFormat = jsonFormat3(ImportOrientDbGraphReturn)
}
import org.trustedanalytics.atk.plugins.orientdbimport.importOrientDbGraphJsonFormat._

@PluginDoc(oneLine = "Imports a graph from OrientDB",
  extended =".",
  returns ="ATK graph.")
class ImportOrientDbGraphPlugin extends SparkCommandPlugin [ImportOrientDbGraphArgs, GraphEntity]{

  override def name: String = "graph:/_import_orientdb"

  override def execute(arguments: ImportOrientDbGraphArgs)(implicit invocation: Invocation):GraphEntity= ???
  /*  // Get OrientDB configurations
    val dbConfig = DbConfigReader.extractConfigurations(arguments.dbName)
    val orientDbGraph = GraphDbFactory.graphDbConnector(dbConfig)
    val vertexFrameRdd = importVertexFrame(orientDbGraph)
    val edgeFrameRdd = importEdgeFrame(orientDbGraph)
    ImportOrientDbGraphReturn(vertexFrameRdd,edgeFrameRdd,dbConfig.dbUri)
  }

  def importVertexFrame(graph: OrientGraphNoTx): VertexFrameRdd ={

    val schemaReader = new SchemaReader(graph)
    val vertexSchema = schemaReader.importVertexSchema()
    val vertexBuffer = new ArrayBuffer[Row]()
    val vertexCount = graph.countVertices(graph.getVertexBaseType.getName)
    var vertexId = 1
    while(vertexCount !=0 && vertexId <= vertexCount) {
      val vertexReader = new VertexReader(graph, vertexSchema, vertexId)
      val vertex = vertexReader.importVertex()
      vertexBuffer += vertex.row
      vertexId += 1
    }
    vertexBuffer.toList
    val rowRdd = sc.parallelize(vertexBuffer.toList)
    new VertexFrameRdd(vertexSchema, rowRdd)
  }

  def importEdgeFrame(graph: OrientGraphNoTx): EdgeFrameRdd = {

    val schemaReader = new SchemaReader(graph)
    val edgeSchema = schemaReader.importEdgeSchema()
    val edgeBuffer = new ArrayBuffer[Row]()
    val edgeCount = graph.countEdges(graph.getVertexBaseType.getName)
    var count = 1
    while(edgeCount !=0 && count <= edgeCount) {
      val srcVertexId = graph.getEdges.iterator().next().getProperty(GraphSchema.srcVidProperty)
      val edgeReader = new EdgeReader(graph, edgeSchema, srcVertexId)
      val edge = edgeReader.importEdge()
      edgeBuffer += edge.row
      count += 1
    }
    edgeBuffer.toList
    val rowRdd = sc.parallelize(edgeBuffer.toList)
    new EdgeFrameRdd(edgeSchema, rowRdd)
  }*/

}
