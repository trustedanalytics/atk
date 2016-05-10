package org.trustedanalytics.atk.plugins.orientdbimport

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.atk.graph.Vertex
import org.trustedanalytics.atk.domain.schema.{ VertexSchema,GraphSchema}
import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex }

class VertexReader(graph: OrientGraphNoTx, vertexSchema: VertexSchema, vertexId: Long) {

  /**
    *
    * @return
    */
  def importVertex():Vertex ={
    try{
    val orientVertex = getOrientVertex
    createVertex(orientVertex)
  }catch{
      case e: Exception =>
        throw new RuntimeException(s"Unable to read vertex with ID $vertexId from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
    *
    * @param orientVertex
    * @return
    */
  def createVertex(orientVertex: BlueprintsVertex): Vertex = {
    val vertex: Vertex = null
    vertexSchema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        vertex.addOrSetValue(col.name, orientVertex.getProperty(col.name))
      }
    })
    vertex
  }

  /**
    *
    * @return
    */
  def getOrientVertex: BlueprintsVertex  = {
    val vertexIterator = graph.getVertices(GraphSchema.vidProperty, vertexId).iterator()
    val orientVertex = vertexIterator.next()
    orientVertex
  }
}
