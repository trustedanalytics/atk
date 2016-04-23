/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.atk.plugins.orientdb

import org.apache.spark.atk.graph.VertexFrameRdd
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema.{ VertexSchema, DataTypes, GraphSchema, Column }
import org.trustedanalytics.atk.testutils.{ TestingOrientDb, TestingSparkContextWordSpec }

/**
 * scala test for VertexFrameWriter, checking the number of exported vertices
 */
class VertexFrameWriterTest extends WordSpec with TestingSparkContextWordSpec with Matchers with TestingOrientDb with BeforeAndAfterEach {
  override def beforeEach() {
    setupOrientDb()
  }

  override def afterEach() {
    cleanupOrientDb()
  }

  "vertex frame writer" should {
    "export vertex frame to OrientDB" in {
      val dbName = "OrientDbTest"
      val dbConfig = new DbConfigurations(dbUri, "admin", "admin")
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
      val vertexFrameWriter = new VertexFrameWriter(vertexFrameRdd, dbConfig)
      val verticesCount = vertexFrameWriter.exportVertexFrame(dbName, batchSize)
      val loadedVerticesCount = orientFileGraph.countVertices()
      verticesCount shouldEqual (loadedVerticesCount)

    }
  }

}
