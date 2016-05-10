package org.trustedanalytics.atk.plugins.orientdbimport

import org.apache.spark.atk.graph.Vertex
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest._
import org.trustedanalytics.atk.domain.schema.{VertexSchema, DataTypes, GraphSchema, Column}
import org.trustedanalytics.atk.plugins.orientdb.VertexWriter
import org.trustedanalytics.atk.testutils.TestingOrientDb

class VertexReaderTest extends WordSpec with TestingOrientDb with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDbInMemory()
  }
  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "Vertex reader" should {
    val vertex = {
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64),
        Column(GraphSchema.labelProperty, DataTypes.string),
        Column("name", DataTypes.string), Column("from", DataTypes.string),
        Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      Vertex(schema, row)
    }

    "get OrientDB vertex" in {
      val vertexId = 1L
      val addOrientVertex = new VertexWriter(orientMemoryGraph)
      val orientVertex = addOrientVertex.addVertex(vertex)
      val vertexReader = new VertexReader(orientMemoryGraph, vertex.schema, vertexId)
      // call method under test
      val orientDbVertex = vertexReader.getOrientVertex
      // validate results
      val nameProp:Any = orientDbVertex.getProperty("name")
      val fromProp :Any = orientDbVertex.getProperty("from")
      val toProp :Any = orientDbVertex.getProperty("to")
      val fairProp: Any = orientDbVertex.getProperty("fair")
      assert(nameProp == "Bob")
      assert(fromProp == "PDX")
      assert(toProp == "LAX")
      assert(fairProp == 350)
    }
     "create vertex" in{

       val vertexId = 1L
       val addOrientVertex = new VertexWriter(orientMemoryGraph)
       val orientVertex = addOrientVertex.addVertex(vertex)
       val vertexReader = new VertexReader(orientMemoryGraph, vertex.schema, vertexId)
       //call method under test
       val atkVertex = vertexReader.createVertex(orientVertex)
       //validate results
        assert(atkVertex.hasProperty("name"))
    }
    /*  "import vertex" in{

     }*/
  }
}
