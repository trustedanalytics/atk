package org.trustedanalytics.atk.plugins.orientdb

import com.tinkerpop.blueprints.{ Parameter, Vertex => BlueprintsVertex }
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphNoTx, OrientEdgeType, OrientVertexType, OrientGraph }
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, EdgeSchema, VertexSchema }

/**
 * Export schema to OrientDB schema
 *
 * @param orientGraph OrientDB graph
 */
class SchemaWriter(orientGraph: OrientGraphNoTx) {

  /**
   * A method to export vertex schema
   *
   * @param vertexSchema atk vertex schema
   * @return OrientDB vertex type
   */
  def createVertexSchema(vertexSchema: VertexSchema): OrientVertexType = {
    val vColumns = vertexSchema.columns
    val className: String = vertexSchema.label
    val orientVertexType = orientGraph.createVertexType(className)
    vColumns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        val orientColumnDataType = OrientDbTypeConverter.convertDataTypeToOrientDbType(col.dataType)
        orientVertexType.createProperty(col.name, orientColumnDataType)
      }
    })
    orientGraph.createKeyIndex(GraphSchema.vidProperty, classOf[BlueprintsVertex], new Parameter("class", className), new Parameter("type", "UNIQUE"))
    orientVertexType
  }

  /**
   * A method to export the edge schema
   *
   * @param edgeSchema ATK edge schema
   * @return OrientDB edge type
   */
  def createEdgeSchema(edgeSchema: EdgeSchema): OrientEdgeType = {
    val className: String = edgeSchema.label
    val oEdgeType = orientGraph.createEdgeType(className)
    val eColumns = edgeSchema.columns
    eColumns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        val orientColumnDataType = OrientDbTypeConverter.convertDataTypeToOrientDbType(col.dataType)
        oEdgeType.createProperty(col.name, orientColumnDataType)
      }
    })
    oEdgeType
  }

}
