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
import org.apache.spark.SparkContext
import org.apache.spark.atk.graph.VertexFrameRdd
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer

/**
 * imports a vertex class from OrientDB database to ATK
 *
 * @param graph OrientDB database
 */
class LoadVertexFrame(graph: OrientGraphNoTx) {

  /**
   * A method imports a vertex class from OrientDB to ATK
   *
   * @return vertex frame RDD
   */
  def importOrientDbVertexClass(sc: SparkContext): VertexFrameRdd = {
    val schemaReader = new SchemaReader(graph)
    val vertexSchema = schemaReader.importVertexSchema()
    val vertexBuffer = new ArrayBuffer[Row]()
    val vertexCount = graph.countVertices(vertexSchema.label)
    var vertexId = 1
    while (vertexCount != 0 && vertexId <= vertexCount) {
      val vertexReader = new VertexReader(graph, vertexSchema, vertexId)
      val vertex = vertexReader.importVertex()
      vertexBuffer += vertex.row
      vertexId += 1
    }
    vertexBuffer.toList
    val rowRdd = sc.parallelize(vertexBuffer.toList)
    new VertexFrameRdd(vertexSchema, rowRdd)
  }
}
