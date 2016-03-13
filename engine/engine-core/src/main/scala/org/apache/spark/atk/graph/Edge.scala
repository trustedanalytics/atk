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

import org.trustedanalytics.atk.graphbuilder.elements.{ Property => GBProperty }
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, EdgeSchema, DataTypes }
import org.trustedanalytics.atk.engine.frame.{ AbstractRow }
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.graphbuilder.elements.GBEdge

/**
 * Edge: self contained edge with complete schema information included.
 * Edge is used when you want RDD's of mixed edge types.
 */
case class Edge(override val schema: EdgeSchema, override var row: Row) extends AbstractEdge with Serializable

/**
 * EdgeWrapper: container that can be re-used to minimize memory usage but still provide a rich API
 * EdgeWrapper is used when you have RDD's of all one edge type (e.g. frame-like operations)
 * With a wrapper, the user sets the row data before each operation.
 * You never want to create RDD[EdgeWrapper] because it wouldn't have data you wanted.
 */
class EdgeWrapper(override val schema: EdgeSchema) extends AbstractEdge with Serializable {

  @transient override var row: Row = null

  /**
   * Set the data in this wrapper
   * @param row the data to set inside this Wrapper
   * @return this instance
   */
  def apply(row: Row): EdgeWrapper = {
    this.row = row
    this
  }

  def toEdge: Edge = {
    new Edge(schema, row)
  }

}

/**
 * AbstractEdge allows two implementations with slightly different trade-offs
 *
 * 1) Edge: self contained edge with complete schema information included.
 *    Edge is used when you want RDD's of mixed edge types.
 *
 * 2) EdgeWrapper: container that can be re-used to minimize memory usage but still provide a rich API
 *    EdgeWrapper is used when you have RDD's of all one edge type (e.g. frame-like operations)
 *    With a wrapper, the user sets the row data before each operation.
 *    You never want to create RDD[EdgeWrapper] because it wouldn't have data you wanted.
 *
 * This is the "common interface" for edges within our system.
 */
trait AbstractEdge extends AbstractRow with Serializable {
  require(schema.isInstanceOf[EdgeSchema], "schema should be for edges")
  require(schema.hasColumnWithType(GraphSchema.edgeProperty, DataTypes.int64), "schema did not have int64 _eid column: " + schema.columnTuples)
  require(schema.hasColumnWithType(GraphSchema.srcVidProperty, DataTypes.int64), "schema did not have int64 _src_vid column: " + schema.columnTuples)
  require(schema.hasColumnWithType(GraphSchema.destVidProperty, DataTypes.int64), "schema did not have int64 _dest_vid column: " + schema.columnTuples)
  require(schema.hasColumnWithType(GraphSchema.labelProperty, DataTypes.str), "schema did not have string _label column: " + schema.columnTuples)

  /**
   * Return id of the edge
   * @return edge id
   */
  def eid(): Long = longValue(GraphSchema.edgeProperty)

  /**
   * Return id of the source vertex
   * @return source vertex id
   */
  def srcVertexId(): Long = longValue(GraphSchema.srcVidProperty)

  /**
   * Return id of the destination vertex
   * @return destination vertex id
   */
  def destVertexId(): Long = longValue(GraphSchema.destVidProperty)

  /**
   * Return label of the edge
   * @return label of the edge
   */
  def label(): String = stringValue(GraphSchema.labelProperty)

  /**
   * Set the label on this vertex
   */
  def setLabel(label: String): Row = {
    setValue(GraphSchema.labelProperty, label)
  }

  def setSrcVertexId(vid: Long): Row = {
    setValue(GraphSchema.srcVidProperty, vid)
  }

  def setDestVertexId(vid: Long): Row = {
    setValue(GraphSchema.destVidProperty, vid)
  }

  /**
   * Create the value of this edge from the supplied GBEdge
   */
  def create(edge: GBEdge): Row = {
    create()
    edge.properties.foreach(prop => setValue(prop.key, prop.value))
    if (edge.eid.isDefined) {
      setValue(GraphSchema.edgeProperty, edge.eid.get)
    }
    setSrcVertexId(edge.tailPhysicalId.asInstanceOf[Long])
    setDestVertexId(edge.headPhysicalId.asInstanceOf[Long])
    setLabel(edge.label)
    row
  }

  /**
   * Convert this row to a GBEdge
   */
  def toGbEdge: GBEdge = createGBEdge(reversed = false)

  /**
   * Convert this row to a GBEdge that has the source and destination vertices reversed
   */
  def toReversedGbEdge: GBEdge = createGBEdge(reversed = true)

  /**
   * create a GBEdge object from this row
   * @param reversed: if true this will reverse the source and destination vids. This is used with a bidirect graph.
   *
   */
  private def createGBEdge(reversed: Boolean): GBEdge = {
    val filteredColumns = schema.columnsExcept(List(GraphSchema.labelProperty, GraphSchema.srcVidProperty, GraphSchema.destVidProperty))
    val properties = filteredColumns.map(column => GBProperty(column.name, value(column.name)))
    // TODO: eid() will be included as a property, is that good enough?
    val srcProperty: GBProperty = GBProperty(GraphSchema.vidProperty, srcVertexId())
    val destProperty: GBProperty = GBProperty(GraphSchema.vidProperty, destVertexId())
    if (reversed)
      GBEdge(None, destProperty.value, srcProperty.value, destProperty, srcProperty, schema.asInstanceOf[EdgeSchema].label, properties.toSet)

    else
      GBEdge(None, srcProperty.value, destProperty.value, srcProperty, destProperty, schema.asInstanceOf[EdgeSchema].label, properties.toSet)
  }
}
