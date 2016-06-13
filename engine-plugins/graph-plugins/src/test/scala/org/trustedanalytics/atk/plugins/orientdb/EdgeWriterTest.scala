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

import org.apache.spark.atk.graph.{ Edge, Vertex }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.testutils.{ TestingOrientDb, TestingSparkContextWordSpec }

class EdgeWriterTest extends WordSpec with Matchers with TestingSparkContextWordSpec with TestingOrientDb with BeforeAndAfterEach {
  override def beforeEach() {
    setupOrientDbInMemory()
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }
  "Edge writer" should {
    "export edge to Orient edge" in {
      //create the source and destination vertices
      val columns = List(
        Column(GraphSchema.vidProperty, DataTypes.int64),
        Column(GraphSchema.labelProperty, DataTypes.string),
        Column("name", DataTypes.string),
        Column("from", DataTypes.string),
        Column("to", DataTypes.string),
        Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val rowSrc = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      val rowDest = new GenericRow(Array(2L, "l1", "Alice", "SFO", "SEA", 465))
      val vertexSrc = Vertex(schema, rowSrc)
      val vertexDest = Vertex(schema, rowDest)
      val vertexWriter = new VertexWriter(orientMemoryGraph)
      val orientVertexSrc = vertexWriter.addVertex(vertexSrc)
      val orientVertexDest = vertexWriter.addVertex(vertexDest)
      // create the edge
      val edgeColumns = List(
        Column(GraphSchema.edgeProperty, DataTypes.int64),
        Column(GraphSchema.srcVidProperty, DataTypes.int64),
        Column(GraphSchema.destVidProperty, DataTypes.int64),
        Column(GraphSchema.labelProperty, DataTypes.string),
        Column("distance", DataTypes.int32))
      val edgeSchema = new EdgeSchema(edgeColumns, "label", "srclabel", "destlabel")
      val edgeRow = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
      val edge = Edge(edgeSchema, edgeRow)
      val edgeWriter = new EdgeWriter(orientMemoryGraph, edge)
      // call method under test
      val orientEdge = edgeWriter.addEdge(orientVertexSrc, orientVertexDest)
      //validate results
      val srcVidProp: Any = orientEdge.getProperty(GraphSchema.srcVidProperty)
      val destVidProp: Any = orientEdge.getProperty(GraphSchema.destVidProperty)
      val edgeProp: Any = orientEdge.getProperty("distance")
      assert(srcVidProp == 2)
      assert(destVidProp == 3)
      assert(edgeProp == 500)
    }
  }
}
