package org.trustedanalytics.atk.plugins.orientdb

import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.impls.orient._
import com.tinkerpop.blueprints.{Parameter, Vertex => BlueprintsVertex}
import org.apache.spark.atk.graph.{Edge, Vertex, VertexFrameRdd}
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.domain.schema.{EdgeSchema, GraphSchema, VertexSchema}
import org.trustedanalytics.atk.engine.frame.RowWrapper

/**
  * Created by wtaie on 4/1/16.
  */
//TODO: update scala doc  , adding the try catch

object ExportOrientDbFunctions extends Serializable {


  def exportVertexFrame(oGraph: OrientGraph, vertexFrame: VertexFrameRdd, batchSize: Int): RDD[Long] = {

    require(oGraph != null, "The Orient graph database instance must not equal null")

    val verticesCountRdd = vertexFrame.mapPartitionVertices(iter => {
      var batchCounter = 0L
      while (iter.hasNext) {
        val vertexWrapper = iter.next()
        val vertex = vertexWrapper.toVertex
        val oVertex = addVertex(oGraph, vertex)
        batchCounter + 1
        // commit the transcation once the batchSize limit is achieved
        if (batchCounter % batchSize == 0 && batchCounter != 0) {
          oGraph.commit()
        }
      }
      oGraph.commit()
      Array(batchCounter).toIterator
    })
    verticesCountRdd
  }

  //  //TODO: complete the method exportEdgeFrame
  //  def exportEdgeFrame(oGraph: OrientGraph, edgeFrame:EdgeFrameRdd, batchSize: Int): RDD[OrientEdge] = ???

  /**
    *
    * @param oGraph       an instance of Orient graph database
    * @param vertexSchema ATK vertex schema
    * @return Orient vertex schema
    */
  def createVertexSchema(oGraph: OrientGraph, vertexSchema: VertexSchema): OrientVertexType = {

    val vColumns = vertexSchema.columns
    val className: String = vertexSchema.label
    val oVertexType = oGraph.createVertexType(className, "V")
    vColumns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        val oColumnDataType = convertDataTypeToOrientDbType(col.dataType)
        oVertexType.createProperty(col.name, oColumnDataType)
      }
    })
    // create key index
    oGraph.createKeyIndex("_vid", classOf[BlueprintsVertex], new Parameter("class", className), new Parameter("type", "UNIQUE"))
    oVertexType
  }

  /**
    *
    * @param oGraph     an instance of Orient graph database
    * @param edgeSchema ATK edge schema
    * @return Orient edge schema
    */

  def createEdgeSchema(oGraph: OrientGraph, edgeSchema: EdgeSchema): OrientEdgeType = {
    val className: String = edgeSchema.label
    val oEdgeType = oGraph.createEdgeType(className)
    val eColumns = edgeSchema.columns
    eColumns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        val oColumnDataType = convertDataTypeToOrientDbType(col.dataType)
        oEdgeType.createProperty(col.name, oColumnDataType)
      }
    })
    oEdgeType
  }

  /**
    *
    * @param dataType ATK data types
    * @return OrientDB data type
    */
  def convertDataTypeToOrientDbType(dataType: DataType): OType = {
    dataType match {
      case int32 => OType.INTEGER
      case int64 => OType.INTEGER
      case float32 => OType.FLOAT
      case float64 => OType.FLOAT
      case string => OType.STRING
      case _ => throw new IllegalArgumentException(s"unsupported type $dataType")
    }
  }

  /**
    *
    * @param oGraph an instance of Orient graph database
    * @param vertex atk vertex to be converted to Orient BlueprintsVertex
    * @return Orient BlueprintsVertex
    */
  def addVertex(oGraph: OrientGraph, vertex: Vertex): BlueprintsVertex = {
    require(oGraph != null, "The Orient graph database instance must not equal null")
    val className: String = vertex.schema.label
    if (oGraph.getVertexType(className) == null) {
      val oVertexType = createVertexSchema(oGraph, vertex.schema)
    }
    val oVertex: BlueprintsVertex = oGraph.addVertex("class:" + className, null)
    val rowWrapper = new RowWrapper(vertex.schema)

    vertex.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        oVertex.setProperty(col.name, rowWrapper(vertex.row).value(col.name))
      }
    })
    oVertex
  }

  /**
    *
    * @param oGraph     an instance of Orient graph database
    * @param edge       he atk edge that is required to be exported to OrientEdge
    * @param srcVertex  is a blueprintsVertex as a source
    * @param destVertex is a blueprintsVertex as a destination
    * @return OrientEdge
    */

  def addEdge(oGraph: OrientGraph, edge: Edge, srcVertex: BlueprintsVertex, destVertex: BlueprintsVertex): OrientEdge = {

    require(oGraph != null, "The Orient graph database instance must not equal null")
    val oEdgeType = createEdgeSchema(oGraph, edge.schema)
    val rowWrapper = new RowWrapper(edge.schema)
    val oEdge: OrientEdge = oGraph.addEdge("class:" + oEdgeType.getName, srcVertex, destVertex, oEdgeType.getName)
    edge.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        oEdge.setProperty(col.name, rowWrapper(edge.row).value(col.name))
      }
    })
    oEdge
  }
}