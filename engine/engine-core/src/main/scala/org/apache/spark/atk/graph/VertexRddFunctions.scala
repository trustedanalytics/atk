/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.apache.spark.atk.graph

import org.trustedanalytics.atk.domain.schema.{ VertexSchema, Schema }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * Additional functions for RDD's of type Vertex
 * @param parent the RDD to wrap
 */
class VertexRddFunctions(parent: RDD[Vertex]) {

  /**
   * Vertex frames from parent based on the labels provided
   * @param schemas by providing the list of schemas you can prevent an extra map/reduce to collect them
   * @return the VertexFrameRdd - one vertex type per RDD
   */
  def splitByLabel(schemas: List[VertexSchema]): List[VertexFrameRdd] = {
    parent.cache()

    val split = schemas.map(_.label).map(label => parent.filter(vertex => vertex.label == label))
    split.foreach(_.cache())

    val rowRdds = split.map(rdd => rdd.map(vertex => vertex.row))

    val results = schemas.zip(rowRdds).map { case (schema: Schema, rows: RDD[Row]) => new VertexFrameRdd(schema, rows) }

    parent.unpersist(blocking = false)
    split.foreach(_.unpersist(blocking = false))
    results
  }

  /**
   * Split parent (mixed vertex types) into a list of vertex frames (one vertex type per frame)
   *
   * IMPORTANT! does not perform as well as splitByLabel(schemas) - extra map() and distinct()
   */
  def splitByLabel(): List[VertexFrameRdd] = {
    parent.cache()
    val schemas = parent.map(vertex => vertex.schema.asInstanceOf[VertexSchema]).distinct().collect().toList
    splitByLabel(schemas)
  }

}
