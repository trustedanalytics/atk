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

import org.trustedanalytics.atk.graphbuilder.elements.{ GBVertex, Property => GBProperty }
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, VertexSchema, DataTypes }
import org.trustedanalytics.atk.engine.frame.AbstractRow
import org.apache.spark.sql.Row

/**
 * Vertex: self contained vertex with complete schema information included.
 * Vertex is used when you want RDD's of mixed vertex types.
 */
case class Vertex(override val schema: VertexSchema, override var row: Row) extends AbstractVertex

/**
 * VertexWrapper: container that can be re-used to minimize memory usage but still provide a rich API
 * VertexWrapper is used when you have RDD's of all one vertex type (e.g. frame-like operations)
 */
class VertexWrapper(override val schema: VertexSchema) extends AbstractVertex with Serializable {

  @transient override var row: Row = null

  /**
   * Set the data in this wrapper
   * @param row the data to set inside this Wrapper
   * @return this instance
   */
  def apply(row: Row): VertexWrapper = {
    this.row = row
    this
  }

  /**
   * Convert to a Vertex
   */
  def toVertex: Vertex = {
    new Vertex(schema, row)
  }
}

/**
 * AbstractVertex allows two implementations with different trade-offs
 *
 * 1) Vertex: self contained vertex with complete schema information included.
 *    Vertex is used when you want RDD's of mixed vertex types.
 *
 * 2) VertexWrapper: container that can be re-used to minimize memory usage but still provide a rich API
 *    VertexWrapper is used when you have RDD's of all one vertex type (e.g. frame-like operations)
 *    With a wrapper, the user sets the row data before each operation.  You never want RDD[VertexWrapper]
 *
 * This is the "common interface" for vertices within our system.
 */
trait AbstractVertex extends AbstractRow {
  require(schema.isInstanceOf[VertexSchema], "schema should be for vertices, vertexSchema was not define")
  require(schema.hasColumnWithType(GraphSchema.vidProperty, DataTypes.int64), "schema did not have int64 _vid column: " + schema.columnTuples)
  require(schema.hasColumnWithType(GraphSchema.labelProperty, DataTypes.str), "schema did not have int64 _label column: " + schema.columnTuples)

  /**
   * Return id of the edge
   * @return edge id
   */
  def vid: Long = longValue(GraphSchema.vidProperty)

  /**
   * Return label of the edge
   * @return label of the edge
   */
  def label: String = stringValue(GraphSchema.labelProperty)

  /**
   * The value for the user defined ID column
   */
  def idValue: Any = value(schema.asInstanceOf[VertexSchema].idColumnName.getOrElse(throw new RuntimeException("id column has not yet been defined in schema: " + schema)))

  def setVid(vid: Long): Row = {
    setValue(GraphSchema.vidProperty, vid)
  }

  /**
   * Set the label on this vertex
   */
  def setLabel(label: String): Row = {
    setValue(GraphSchema.labelProperty, label)
  }

  override def create(vertex: GBVertex): Row = {
    create()
    vertex.properties.foreach(prop => setValue(prop.key, prop.value))
    setVid(vertex.physicalId.asInstanceOf[Long])
    row
  }

  /**
   * Convert this row to a GbVertex
   */
  def toGbVertex: GBVertex = {
    val properties = schema.columnsExcept(List(GraphSchema.vidProperty)).map(column => GBProperty(column.name, value(column.name)))
    GBVertex(vid, GBProperty(GraphSchema.vidProperty, vid), properties.toSet)
  }

  /**
   * Merge values from other row onto this row.
   *
   * Values in this row are preferred, only missing values are copies from supplied row
   */
  // TODO: weird compile issues with this, delete or fix
  //  def merge(otherRow: Row): Row = {
  //    val idIndex = schema.columnIndex(schema.vertexSchema.get.idColumnName)
  //    val labelIndex = schema.columnIndex(GraphSchema.labelProperty)
  //    require(row(idIndex) == otherRow(idIndex), "vertices with different ids can't be merged")
  //    require(row(labelIndex) == otherRow(labelIndex), "vertices with different labels can't be merged")
  //    val content = row.toArray.zip(otherRow.toArray).map {
  //      case (first: Any, second: Any) =>
  //        first match {
  //          case null => second
  //          case None => second
  //          case _ => first
  //        }
  //    }
  //    new GenericRow(content)
  //  }
}

object Vertex {

  // TODO: this was written but then didn't get used, if we don't need soon then we should delete it. --Todd 11/13/2014
  //  /**
  //   * Create a Vertex from a GBVertex
  //   *
  //   * It is better not to use GBVertices because this conversion requires inferring the schema from the data.
  //   */
  //  def toVertex(gbVertex: GBVertex): Vertex = {
  //
  //    // TODO: not sure the correct behavior: should we force certain fields defined idColumn, _label, _vid or make them optional?
  //
  //    val schema = toSchema(gbVertex)
  //    val content = new Array[Any](schema.columns.length)
  //    //TODO: what is the right way to introduce GenericMutableRow?
  //    val row = new GenericRow(content)
  //
  //    val vertex = new Vertex(schema, row)
  //    if (gbVertex.physicalId != null) {
  //      vertex.setVid(gbVertex.physicalId.asInstanceOf[Long])
  //    }
  //    vertex.setLabel(gbVertex.getPropertyValueAsString("_label"))
  //    val excluded = Set("_vid", "_label")
  //    val props = gbVertex.fullProperties.filterNot(prop => excluded.contains(prop.key))
  //    props.foreach(property => vertex.setValue(property.key, property.value))
  //    vertex
  //  }
  //
  //  /**
  //   * Create a schema from a GBVertex
  //   * @param gbVertex
  //   * @return the schema for the supplied schema
  //   */
  //  def toSchema(gbVertex: GBVertex): Schema = {
  //
  //    // TODO: not sure the correct behavior: should we force certain fields defined idColumn, _label, _vid or make them optional?
  //
  //    if (gbVertex.getProperty("_label").isEmpty) {
  //      throw new IllegalArgumentException("GBVertex did not have the required _label property " + gbVertex)
  //    }
  //    val label = gbVertex.getPropertyValueAsString("_label")
  //    val idColumnName: Option[String] = if (gbVertex.gbId != null) {
  //      Some(gbVertex.gbId.key)
  //    }
  //    else {
  //      None
  //    }
  //    val vertexSchema = VertexSchema(label, idColumnName)
  //    val schema = new Schema(gbVertex.fullProperties.map(prop => (prop.key, DataTypes.dataTypeOfValue(prop.value))).toList)
  //      .copy(vertexSchema = Some(vertexSchema))
  //    schema
  //  }
}
