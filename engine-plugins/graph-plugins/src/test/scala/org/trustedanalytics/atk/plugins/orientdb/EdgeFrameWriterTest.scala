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

import org.apache.spark.atk.graph.{ EdgeFrameRdd, VertexFrameRdd }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.testutils.{ TestingOrientDb, TestingSparkContextWordSpec }

/**
 *  scala test for EdgeFrameWriter, checking the number of exported edges
 */
class EdgeFrameWriterTest extends WordSpec with TestingSparkContextWordSpec with TestingOrientDb with Matchers with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDb()
  }

  override def afterEach() {
    cleanupOrientDb()
  }

  "Edge frame writer" should {
    "Export edge frame" in {
      // exporting a vertex frame:
      val dbName = "OrientDbTest"
      val dbConfig = new DbConfigurations(dbUri, "admin", "admin", "port", "host")
      val vColumns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val vSchema = new VertexSchema(vColumns, GraphSchema.labelProperty, null)

      val vertices: List[Row] = List(
        new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350)),
        new GenericRow(Array(2L, "l1", "Alice", "SFO", "SEA", 465)),
        new GenericRow(Array(3L, "l1", "Fred", "NYC", "PIT", 675)),
        new GenericRow(Array(4L, "l1", "Lucy", "LAX", "PDX", 450)))
      val vRowRdd = sparkContext.parallelize(vertices)
      val vertexFrameRdd = new VertexFrameRdd(vSchema, vRowRdd)
      val vBatchSize = 4
      val vertexFrameWriter = new VertexFrameWriter(vertexFrameRdd, dbConfig)
      val verticesCountRdd = vertexFrameWriter.exportVertexFrame(vBatchSize)

      //exporting the edge frame:
      val eColumns = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("distance", DataTypes.int32))
      val eSchema = new EdgeSchema(eColumns, "label", "srclabel", "destlabel")
      val edges: List[Row] = List(
        new GenericRow(Array(1L, 1L, 2L, "distance1", 100)),
        new GenericRow(Array(2L, 2L, 3L, "distance2", 200)),
        new GenericRow(Array(3L, 3L, 4L, "distance3", 400)))
      val eRowRdd = sparkContext.parallelize(edges)
      val edgeFrameRdd = new EdgeFrameRdd(eSchema, eRowRdd)
      val batchSize = 3
      val edgeFrameWriter = new EdgeFrameWriter(edgeFrameRdd, dbConfig)
      val edgesCount = edgeFrameWriter.exportEdgeFrame(batchSize)
      val loadedEdgesCount = orientFileGraph.countEdges()
      edgesCount shouldEqual (3)

    }
  }
}
