package org.trustedanalytics.atk.plugins.orientdb

import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.impls.orient._
import com.tinkerpop.blueprints.{ Parameter, Vertex => BlueprintsVertex }
import org.apache.spark.atk.graph.{ Edge, EdgeFrameRdd, Vertex, VertexFrameRdd }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.domain.schema.{ DataTypes, EdgeSchema, GraphSchema, VertexSchema }
import org.trustedanalytics.atk.engine.frame.RowWrapper

/**
 * Created by wtaie on 4/1/16.
 */

object ExportOrientDbFunctions extends Serializable {

  /**
   * Method to export vertex frame to OrientDb
   * @param dbUri OrientDb URI
   * @param vertexFrameRdd  vertices frame to be exported to Orient
   * @param batchSize the number of vertices to be committed
   * @return the number of exported vertices
   */
  def exportVertexFrame(dbUri: String, vertexFrameRdd: VertexFrameRdd, batchSize: Int): Long = {

    val verticesCountRdd = vertexFrameRdd.mapPartitionVertices(iter => {
      var batchCounter = 0L
      val oGraph = GraphDbFactory.GraphDbConnector(dbUri)
      while (iter.hasNext) {
        val vertexWrapper = iter.next()
        val vertex = vertexWrapper.toVertex
        val oVertex = addVertex(oGraph, vertex)
        batchCounter += 1
        if (batchCounter % batchSize == 0 && batchCounter != 0) {
          oGraph.commit()
        }
      }
      oGraph.shutdown(true, true) // commit and close the graph database
      Array(batchCounter).toIterator
    })
    verticesCountRdd.sum().toLong
  }

  /**
   * Method to export edge frame to OrientDb
   * @param dbUri OrientDb URI
   * @param edgeFrameRdd edges frame to be exported to Orient
   * @param batchSize the number of edges to be commited
   * @return the number of exported edges
   */
  def exportEdgeFrame(dbUri: String, edgeFrameRdd: EdgeFrameRdd, batchSize: Int): Long = {

    val edgesCountRdd = edgeFrameRdd.mapPartitionEdges(iter => {
      val oGraph = GraphDbFactory.GraphDbConnector(dbUri)
      var batchCounter = 0L
      while (iter.hasNext) {
        val edgeWrapper = iter.next()
        val edge = edgeWrapper.toEdge
        // lookup the source and destination vertices
        val srcVertex = oGraph.getVertices("_vid", edge.srcVertexId()).iterator().next()
        val destVertex = oGraph.getVertices("_vid", edge.destVertexId()).iterator().next()
        val oEdge = addEdge(oGraph, edge, srcVertex, destVertex)
        batchCounter += 1
        if (batchCounter % batchSize == 0 && batchCounter != 0) {
          oGraph.commit()
        }
      }
      oGraph.shutdown(true, true) //commit the changes and close the graph
      Array(batchCounter).toIterator
    })
    edgesCountRdd.sum().toLong
  }

  /**
   * Method to export the vertex schema
   * @param oGraph  an instance of Orient graph database
   * @param vertexSchema ATK vertex schema
   * @return Orient vertex schema
   */
  def createVertexSchema(oGraph: OrientGraph, vertexSchema: VertexSchema): OrientVertexType = {

    val vColumns = vertexSchema.columns
    val className: String = "Vertex" + vertexSchema.label
    val oVertexType = oGraph.createVertexType(className)
    vColumns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        val oColumnDataType = DataTypesToOTypeMatch.convertDataTypeToOrientDbType(col.dataType)
        oVertexType.createProperty(col.name, oColumnDataType)
      }
    })
    oGraph.createKeyIndex("_vid", classOf[BlueprintsVertex], new Parameter("class", className), new Parameter("type", "UNIQUE"))
    oVertexType
  }

  /**
   * Method to export the edge schema
   * @param oGraph  an instance of Orient graph database
   * @param edgeSchema ATK edge schema
   * @return Orient edge schema
   */

  def createEdgeSchema(oGraph: OrientGraph, edgeSchema: EdgeSchema): OrientEdgeType = {

    val className: String = "Edge" + edgeSchema.label
    val oEdgeType = oGraph.createEdgeType(className)
    val eColumns = edgeSchema.columns
    eColumns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        val oColumnDataType = DataTypesToOTypeMatch.convertDataTypeToOrientDbType(col.dataType)
        oEdgeType.createProperty(col.name, oColumnDataType)
      }
    })
    oEdgeType
  }

  object DataTypesToOTypeMatch extends App {

    /**
     * Method for converting data types to Orient data types
     * @param dataType ATK data types
     * @return OrientDB data type
     */
    def convertDataTypeToOrientDbType(dataType: DataType): OType = dataType match {
      case int64 if dataType.equalsDataType(DataTypes.int64) => OType.LONG
      case int32 if dataType.equalsDataType(DataTypes.int32) => OType.INTEGER
      case float32 if dataType.equalsDataType(DataTypes.float32) => OType.FLOAT
      case float64 if dataType.equalsDataType(DataTypes.float64) => OType.FLOAT
      case string if dataType.equalsDataType(DataTypes.string) => OType.STRING
      case _ => throw new IllegalArgumentException(s"unsupported type $dataType")
    }
  }

  /**
   * Method for exporting a vertex
   * @param oGraph an instance of Orient graph database
   * @param vertex atk vertex to be converted to Orient BlueprintsVertex
   * @return Orient BlueprintsVertex
   */
  def addVertex(oGraph: OrientGraph, vertex: Vertex): BlueprintsVertex = {
    require(oGraph != null, "The Orient graph database instance must not equal null")
    val className: String = "Vertex" + vertex.schema.label
    if (oGraph.getVertexType(className) == null) {
      val oVertexType = createVertexSchema(oGraph, vertex.schema)
    }
    val oVertex: BlueprintsVertex = oGraph.addVertex(className, null)
    val rowWrapper = new RowWrapper(vertex.schema)
    vertex.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        oVertex.setProperty(col.name, rowWrapper(vertex.row).value(col.name))
      }
    })
    oVertex
  }

  /**
   * Method for exporting an edge
   * @param oGraph     an instance of Orient graph database
   * @param edge       he atk edge that is required to be exported to OrientEdge
   * @param srcVertex  is a blueprintsVertex as a source
   * @param destVertex is a blueprintsVertex as a destination
   * @return OrientEdge
   */

  def addEdge(oGraph: OrientGraph, edge: Edge, srcVertex: BlueprintsVertex, destVertex: BlueprintsVertex): OrientEdge = {

    require(oGraph != null, "The Orient graph database instance must not equal null")
    val className = "Edge" + edge.schema.label
    if (oGraph.getEdgeType(className) == null) {
      val oEdgeType = createEdgeSchema(oGraph, edge.schema)
    }
    val oEdge = oGraph.addEdge("class:" + className, srcVertex, destVertex, className)
    val rowWrapper = new RowWrapper(edge.schema)
    edge.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        oEdge.setProperty(col.name, rowWrapper(edge.row).value(col.name))
      }
    })
    oEdge
  }
}