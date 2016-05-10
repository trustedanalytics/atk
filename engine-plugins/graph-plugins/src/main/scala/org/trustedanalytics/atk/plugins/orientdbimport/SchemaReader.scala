package org.trustedanalytics.atk.plugins.orientdbimport

import java.util
import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.plugins.orientdb.OrientDbTypeConverter

class SchemaReader(graph:OrientGraphNoTx) {
  val orientSchema = graph.getRawGraph.getMetadata.getSchema

  /**
    *
    * @return
    */
  def importVertexSchema(): VertexSchema = {
    val vertexSchema: VertexSchema = null
    val vertexType = graph.getVertexBaseType
    try {
      val className = vertexType.getName
      val vertexSchema.label = className
      val properties = orientSchema.getClass(className).getIndexedProperties
      createVertexSchema(vertexSchema, properties)
    }catch{
      case e: Exception =>
      throw new RuntimeException(s"Unable to read vertex schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
    *
    * @param vertexSchema
    * @param properties
    * @return
    */
  def createVertexSchema(vertexSchema: VertexSchema, properties: util.Collection[OProperty]): VertexSchema = {
    while (properties.iterator().hasNext) {
      val prop = properties.iterator().next()
      val columnName = prop.getName
      val columnType = OrientDbTypeConverter.convertOrientDbtoDataType(prop.getType)
      vertexSchema.addColumn(columnName, columnType)
    }
    vertexSchema
  }


  /**
    *
    * @return
    */
  def importEdgeSchema(): EdgeSchema = {
    val edgeSchema: EdgeSchema = null
    val edgeType = graph.getEdgeBaseType
    val edgeClassName = edgeType.getName
    try{
      val edgeSchema.label= edgeClassName
      val properties = orientSchema.getClass(edgeClassName).getIndexedProperties
     createEdgeSchema(edgeSchema,properties)
    }catch{
      case e:Exception =>
        throw new RuntimeException(s"Unable to read edge schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
    *
     * @param edgeSchema
    * @param properties
    * @return
    */
  def createEdgeSchema(edgeSchema: EdgeSchema, properties: util.Collection[OProperty]): EdgeSchema = {
    while (properties.iterator().hasNext) {
      val prop = properties.iterator().next()
      val columnName = prop.getName
      val columnType = OrientDbTypeConverter.convertOrientDbtoDataType(prop.getType)
      edgeSchema.addColumn(columnName, columnType)
    }
    edgeSchema
  }

}
