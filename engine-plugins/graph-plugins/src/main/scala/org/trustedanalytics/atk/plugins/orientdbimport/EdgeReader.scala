package org.trustedanalytics.atk.plugins.orientdbimport

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import com.tinkerpop.blueprints.{ Edge => BlueprintsEdge }
import org.apache.spark.atk.graph.Edge
import org.trustedanalytics.atk.domain.schema.{GraphSchema, EdgeSchema}

class EdgeReader (graph: OrientGraphNoTx, edgeSchema: EdgeSchema, srcVertexId:Long){

  /**
    *
    * @return
    */
  def importEdge(): Edge = {
    try{
      val orientEdge = getOrientEdge
      createEdge(orientEdge)
    }catch{
      case e: Exception =>
        throw new RuntimeException(s"Unable to read edge with source ID $srcVertexId from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
    *
    * @param orientEdge
    * @return
    */
  def createEdge(orientEdge: BlueprintsEdge): Edge = {
    val edge: Edge = null
    edgeSchema.columns.foreach(col => {
      edge.addOrSetValue(col.name, orientEdge.getProperty(col.name))
    })
    edge
  }

  /**
    *
    * @return
    */
  def getOrientEdge: BlueprintsEdge  = {
    val edgeIterator = graph.getEdges(GraphSchema.srcVidProperty, srcVertexId).iterator()
    val orientEdge = edgeIterator.next()
    orientEdge
  }
}