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

package org.apache.spark.atk.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.{ EdgeSchema, Schema }

/**
 * Additional functions for RDD's of type Edge
 * @param parent the RDD to wrap
 */
class EdgeRddFunctions(parent: RDD[Edge]) {

  /**
   * Edge frames from parent based on the labels provided
   * @param schemas by providing the list of schemas you can prevent an extra map/reduce to collect them
   * @return the EdgeFrameRdd - one edge type per RDD
   */
  def splitByLabel(schemas: List[EdgeSchema]): List[EdgeFrameRdd] = {
    parent.cache()

    val split = schemas.map(_.label).map(label => parent.filter(edge => edge.label == label))
    split.foreach(_.cache())

    val rowRdds = split.map(rdd => rdd.map(edge => edge.row))

    val results = schemas.zip(rowRdds).map { case (schema: Schema, rows: RDD[Row]) => new EdgeFrameRdd(schema, rows) }

    parent.unpersist(blocking = false)
    split.foreach(_.unpersist(blocking = false))
    results
  }

  /**
   * Split parent (mixed edge types) into a list of edge frames (one edge type per frame)
   *
   * IMPORTANT! does not perform as well as splitByLabel(schemas) - extra map() and distinct()
   */
  def splitByLabel(): List[EdgeFrameRdd] = {
    parent.cache()
    val schemas = parent.map(edge => edge.schema.asInstanceOf[EdgeSchema]).distinct().collect().toList
    splitByLabel(schemas)
  }

}
