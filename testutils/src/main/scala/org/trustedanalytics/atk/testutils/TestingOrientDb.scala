package org.trustedanalytics.atk.testutils

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.{OrientVertex, OrientEdge, OrientGraph}

/**
  * Created by wtaie on 3/31/16.
  */
trait TestingOrientDb {
  /*val vertexClassName: String = "Person"
  val vertexPropNames: Array[String] = Array("firstName", "lastName")
  val vertexPropValues: Array[String] = Array("John", "Smith")
  val vertexClassName1: String = "Address"
  val vertexPropNames1: Array[String] = Array("street", "city", "state")
  val vertexPropValues1: Array[String] = Array("Van Ness Ave.", "San Francisco", "California")
  var v1: Vertex = null
  var v2: Vertex = null*/
  //protected var names: Array[String] = Array[String]("Jay", "Luca", "Bill", "Steve", "Jill", "Luigi", "Enrico", "Emanuele")
  //protected var surnames: Array[String] = Array[String]("Miner", "Ferguson", "Cancelli", "Lavori", "Raggio", "Eagles", "Smiles", "Ironcutter")
  var orientGraph: OrientGraph = null


  def setupOrientDb(): Unit = {
    val uuid = java.util.UUID.randomUUID.toString
    orientGraph = new OrientGraph("memory:OrientTestDb"+uuid)
    /*v1 = createVertex(orientGraph, vertexClassName, vertexPropNames, vertexPropValues)
    v2 = createVertex(orientGraph, vertexClassName1, vertexPropNames1, vertexPropValues1)
    createEdge(orientGraph, "Live", "lives", v1, v2)*/
  }
  /*private def createVertex(ograph: OrientGraph, vertexClassName: String, vertexPropNames: Array[String], vertexPropValues: Array[String]): Vertex =
  {
   // ograph.createVertexType(vertexClassName)
    var v: Vertex = ograph.addVertex("class:" + vertexClassName)
    ograph.begin
    try {
      if (vertexPropNames != null && vertexPropValues != null) {
        val numberOfProp: Int = vertexPropNames.length
        var counter: Int = 1
        while (counter <= numberOfProp) {
          v.setProperty(vertexPropNames(counter), vertexPropValues(counter))
          ograph.commit
          counter += 1
        }
      }
    }
    catch {
      case e: Exception => {
        System.out.println("Unable to commit the create vertex tx ")
        ograph.rollback
      }
    }
    return v
  }

  private def createEdge(graph: OrientGraph, edgeClassName: String, edgeLabel: String, srcVertex: Vertex, destVertex: Vertex) {
    graph.begin
    try {
      val edge_ : OrientEdge = graph.addEdge("class:" + edgeClassName, srcVertex, destVertex, edgeLabel)
      graph.commit
    }
    catch {
      case e: Exception => {
        System.out.println("Unable to commit the create edge tx ")
        graph.rollback
      }
    }
  }*/

  /**
    * commit the transcation and close the ograph
    */
  def cleanupOrientDb(): Unit = {
    try {
      if (orientGraph != null) {
        orientGraph.commit()
      }
    } finally {
      orientGraph.shutdown()
    }
  }
}
