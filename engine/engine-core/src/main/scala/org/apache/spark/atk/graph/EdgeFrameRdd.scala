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

import org.trustedanalytics.atk.domain.schema.{ EdgeSchema, GraphSchema, Schema }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.graphbuilder.elements.GBEdge

import scala.reflect.ClassTag

/**
 * Edge list for a "Seamless" Graph
 *
 * @param schema  the schema describing the columns of this edge frame
 * @param prev the underlying rdd which represents this edge frame
 */
class EdgeFrameRdd(schema: EdgeSchema, prev: RDD[Row]) extends FrameRdd(schema, prev) {

  def this(frameRdd: FrameRdd) = this(frameRdd.frameSchema.asInstanceOf[EdgeSchema], frameRdd)

  def this(schema: Schema, rowRDD: RDD[Row]) = this(schema.asInstanceOf[EdgeSchema], rowRDD)

  /** Edge wrapper provides richer API for working with Vertices */
  val edge = new EdgeWrapper(schema)

  /**
   * Map over edges
   * @param mapFunction map function that operates on a EdgeWrapper
   * @tparam U return type that will be the in resulting RDD
   */
  def mapEdges[U: ClassTag](mapFunction: (EdgeWrapper) => U): RDD[U] = {
    this.map(data => {
      mapFunction(edge(data))
    })
  }

  /**
   * Map partition over edges
   * @param mapPartitionFunction map partition function that operates on an iterator EdgeWrapper
   * @tparam U return type that will be the in resulting RDD
   * @return
   */
  def mapPartitionEdges[U: ClassTag](mapPartitionFunction: Iterator[EdgeWrapper] => Iterator[U]): RDD[U] = {
    this.map(data => edge(data)).mapPartitions(mapPartitionFunction)
  }

  /**
   * Convert this RDD in match the schema provided
   * @param updatedSchema the new schema to take effect
   * @return the new RDD
   */
  override def convertToNewSchema(updatedSchema: Schema): EdgeFrameRdd = {
    if (schema == updatedSchema) {
      // no changes needed
      this
    }
    else {
      // map to new schema
      new EdgeFrameRdd(super.convertToNewSchema(updatedSchema))
    }
  }

  /**
   * Map over all edges and assign the label from the schema
   */
  def assignLabelToRows(): EdgeFrameRdd = {
    new EdgeFrameRdd(schema, mapEdges(edge => edge.setLabel(schema.label)))
  }

  /**
   * Append edges to the current frame:
   * - union the schemas to match, if needed
   * - no overwrite
   */
  def append(other: FrameRdd): EdgeFrameRdd = {
    val unionedSchema = schema.union(other.frameSchema).reorderColumns(GraphSchema.edgeSystemColumnNames)

    // TODO: better way to check for empty?
    if (take(1).length > 0) {
      val part1 = convertToNewSchema(unionedSchema)
      val part2 = new EdgeFrameRdd(other.convertToNewSchema(unionedSchema))
      new EdgeFrameRdd(part1.union(part2)).assignLabelToRows()
    }
    else {
      new EdgeFrameRdd(other.convertToNewSchema(unionedSchema)).assignLabelToRows()
    }
  }

  def toEdgeRdd: RDD[Edge] = {
    this.mapEdges(_.toEdge)
  }

  /**
   * Convert this EdgeFrameRdd to a GB Edge RDD
   */
  def toGbEdgeRdd: RDD[GBEdge] = {
    if (schema.directed)
      this.mapEdges(_.toGbEdge)
    else
      this.mapEdges(_.toGbEdge) union this.mapEdges(_.toReversedGbEdge)
  }

}
