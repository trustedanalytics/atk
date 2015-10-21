/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.apache.spark.atk.graph

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, Schema, VertexSchema }
import org.trustedanalytics.atk.graphbuilder.elements.GBVertex

import scala.reflect.ClassTag

/**
 * Vertex List for a "Seamless" Graph
 *
 * @param schema  the schema describing the columns of this frame
 */
class VertexFrameRdd(schema: VertexSchema, prev: RDD[Row]) extends FrameRdd(schema, prev) {

  def this(frameRdd: FrameRdd) = this(frameRdd.frameSchema.asInstanceOf[VertexSchema], frameRdd)

  /** Vertex wrapper provides richer API for working with Vertices */
  val vertexWrapper = new VertexWrapper(schema)

  /**
   * Merge duplicate Vertices, creating a new Vertex that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  // TODO: implement or delete
  //  def mergeDuplicates(): VertexFrameRdd = {
  //    new VertexFrameRdd(schema, groupVerticesById().mapValues(dups => dups.reduce((m1, m2) => vertex(m1).merge(m2))).values)
  //  }

  /**
   * Drop duplicates based on user defined id
   */
  def dropDuplicates(): VertexFrameRdd = {
    val idColumnName = schema.idColumnName.getOrElse(throw new RuntimeException("Cannot drop duplicates is id column has not yet been defined"))
    val columnNames = List(idColumnName, schema.label)
    this.dropDuplicatesByColumn(columnNames)
  }

  def groupVerticesById() = {
    this.groupBy(data => vertexWrapper(data).idValue)
  }

  /**
   * Update rows in vertex frame
   *
   * @param newRows New rows
   * @return New vertex frame with updated rows
   */
  override def update(newRows: RDD[Row]): Self = {
    (new VertexFrameRdd(this.schema, newRows)).asInstanceOf[Self]

  }

  /**
   * Map over vertices
   * @param mapFunction map function that operates on a VertexWrapper
   * @tparam U return type that will be the in resulting RDD
   */
  def mapVertices[U: ClassTag](mapFunction: (VertexWrapper) => U): RDD[U] = {
    this.map(data => {
      mapFunction(vertexWrapper(data))
    })
  }

  /**
   * RDD of idColumn and _vid
   */
  def idColumns: RDD[(Any, Long)] = {
    mapVertices(vertex => (vertex.idValue, vertex.vid))
  }

  /**
   * Convert this RDD in match the schema provided
   * @param updatedSchema the new schema to take effect
   * @return the new RDD
   */
  override def convertToNewSchema(updatedSchema: Schema): VertexFrameRdd = {
    if (schema == updatedSchema) {
      // no changes needed
      this
    }
    else {
      // map to new schema
      new VertexFrameRdd(super.convertToNewSchema(updatedSchema))
    }
  }

  def assignLabelToRows(): VertexFrameRdd = {
    new VertexFrameRdd(schema, mapVertices(vertex => vertex.setLabel(schema.label)))
  }

  /**
   * Append vertices to the current frame:
   * - overwriting existing vertices, if needed
   * - union the schemas to match, if needed
   * @param preferNewVertexData true to prefer new vertex data, false to prefer existing vertex data - during merge.
   *                            false is useful for createMissingVertices, otherwise you probably always want true.
   */
  def append(other: FrameRdd, preferNewVertexData: Boolean = true): VertexFrameRdd = {
    val unionedSchema = schema.union(other.frameSchema).reorderColumns(GraphSchema.vertexSystemColumnNames).asInstanceOf[VertexSchema]

    val part2 = new VertexFrameRdd(other.convertToNewSchema(unionedSchema)).mapVertices(vertex => (vertex.idValue, (vertex.data, preferNewVertexData)))

    // TODO: better way to check for empty?
    val appended = if (take(1).length > 0) {
      val part1 = convertToNewSchema(unionedSchema).mapVertices(vertex => (vertex.idValue, (vertex.data, !preferNewVertexData)))
      dropDuplicates(part1.union(part2))
    }
    else {
      dropDuplicates(part2)
    }
    new VertexFrameRdd(unionedSchema, appended).assignLabelToRows()
  }

  /**
   * Drop duplicates
   * @param vertexPairRDD a pair RDD of the format (uniqueId: Any, (row: Row, preferred: Boolean))
   * @return rows without duplicates
   */
  private def dropDuplicates(vertexPairRDD: RDD[(Any, (Row, Boolean))]): RDD[Row] = {

    // TODO: do we care about merging?
    vertexPairRDD.reduceByKey {
      case ((row1: Row, row1Preferred: Boolean), (row2: Row, row2Preferred: Boolean)) =>
        if (row1Preferred) {
          // prefer newer data
          (row1, row1Preferred)
        }
        else {
          (row2, row2Preferred)
        }
    }.values.map { case (row: Row, rowNew: Boolean) => row }
  }

  /**
   * Define the ID column name
   */
  def setIdColumnName(name: String): VertexFrameRdd = {
    val updatedVertexSchema = schema.copy(idColumnName = Some(name))
    new VertexFrameRdd(updatedVertexSchema, this)
  }

  def toVertexRDD: RDD[Vertex] = {
    this.mapVertices(_.toVertex)
  }

  /**
   * Convert this VertexFrameRdd to a GB Vertex RDD
   */
  def toGbVertexRDD: RDD[GBVertex] = {
    this.mapVertices(_.toGbVertex)
  }
}
