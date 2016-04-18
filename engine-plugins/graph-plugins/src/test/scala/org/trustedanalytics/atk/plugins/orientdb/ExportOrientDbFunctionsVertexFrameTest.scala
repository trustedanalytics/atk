package org.trustedanalytics.atk.plugins.orientdb

import org.apache.spark.atk.graph.VertexFrameRdd
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ WordSpec, BeforeAndAfterEach, Matchers }
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.testutils.{ TestingOrientDb, TestingSparkContextWordSpec }

/**
 * Created by wtaie on 4/5/16.
 */
class ExportOrientDbFunctionsVertexFrameTest extends WordSpec with TestingSparkContextWordSpec with Matchers with TestingOrientDb with BeforeAndAfterEach {
  override def beforeEach() {
    setupOrientDb()
  }

  override def afterEach() {
    cleanupOrientDb()
  }

  "Export orientDb functions" should {
    "test export vertex frame" in {
      val dbUri: String = "plocal:/home/wtaie/graphDBs_home/orientdb-community-2.1.12/databases/OrientDbTest"
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
      val verticesCount = ExportOrientDbFunctions.exportVertexFrame(dbUri, vertexFrameRdd, batchSize)
      verticesCount shouldEqual (4)
    }
  }

}
