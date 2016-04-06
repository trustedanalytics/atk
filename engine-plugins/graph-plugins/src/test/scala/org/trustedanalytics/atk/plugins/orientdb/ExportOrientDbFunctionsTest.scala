package org.trustedanalytics.atk.plugins.orientdb

import com.tinkerpop.blueprints.{Vertex => BlueprintsVertex}
import org.apache.spark.atk.graph.{Edge, Vertex, VertexFrameRdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.testutils.{TestingOrientDb, TestingSparkContextWordSpec}

/**
  * Created by wtaie on 4/1/16.
  */
class ExportOrientDbFunctionsTest extends WordSpec with Matchers with TestingSparkContextWordSpec with TestingOrientDb with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDb()
  }

  override def afterEach() {
    cleanupOrientDb()
  }

  "Export to OrientDB functions" should {

    "Add vertex" in {
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      val vertex = Vertex(schema, row)
      val oVertex = ExportOrientDbFunctions.addVertex(orientGraph, vertex)
      val vidProp: Any = oVertex.getProperty(GraphSchema.vidProperty)
      val propName: Any = oVertex.getProperty("name")
      val keyIdx = orientGraph.getIndexedKeys(classOf[BlueprintsVertex])

      assert(propName == "Bob")
      assert(vidProp == 1)
      keyIdx should contain("_vid")
    }
    "Add Edge" in {
      //create the source and destination vertices
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val rowSrc = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      val rowDest = new GenericRow(Array(2L, "l1", "Alice", "SFO", "SEA", 465))
      val vertexSrc = Vertex(schema, rowSrc)
      val vertexDest = Vertex(schema, rowDest)
      val oVertexSrc = ExportOrientDbFunctions.addVertex(orientGraph, vertexSrc)
      val oVertexDest = ExportOrientDbFunctions.addVertex(orientGraph, vertexDest)
      // create the edge
      val edgeColumns = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("distance", DataTypes.int32))
      val edgeSchema = new EdgeSchema(edgeColumns, "label", "srclabel", "destlabel")
      val edgeRow = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
      val edge = Edge(edgeSchema, edgeRow)
      val oEdge = ExportOrientDbFunctions.addEdge(orientGraph, edge, oVertexSrc, oVertexDest)
      val srcVidProp: Any = oEdge.getProperty(GraphSchema.srcVidProperty)
      val destVidProp: Any = oEdge.getProperty(GraphSchema.destVidProperty)
      val edgeProp: Any = oEdge.getProperty("distance")
      assert(srcVidProp == 2)
      assert(destVidProp == 3)
      assert(edgeProp == 500)

    }
    /*"test export vertex frame" in {
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)

      val vertices: List[Row] = List(
        new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350)),
        new GenericRow(Array(2L, "l1", "Alice", "SFO", "SEA", 465)),
        new GenericRow(Array(3L, "l1", "Fred", "NYC", "PIT", 675)),
        new GenericRow(Array(4L, "l1", "Lucy", "LAX", "PDX", 450)))
        val batchSize = 4
        val rowRdd = sparkContext.parallelize(vertices)
        val vertexFrameRdd = new VertexFrameRdd(schema, rowRdd)

        val verticesCountRdd = ExportOrientDbFunctions.exportVertexFrame(orientGraph, vertexFrameRdd, batchSize)
        verticesCountRdd.sum() shouldEqual(4.0)

    }*/
  }
}
