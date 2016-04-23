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

/**
 * scala test for EdgeWriter, checking the exported exported edge source vertex ID, destination vertex ID and the edge properties.
 */
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
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val rowSrc = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      val rowDest = new GenericRow(Array(2L, "l1", "Alice", "SFO", "SEA", 465))
      val vertexSrc = Vertex(schema, rowSrc)
      val vertexDest = Vertex(schema, rowDest)
      val addOrientVertex = new VertexWriter(orientMemoryGraph)
      val oVertexSrc = addOrientVertex.addVertex(vertexSrc)
      val oVertexDest = addOrientVertex.addVertex(vertexDest)
      // create the edge
      val edgeColumns = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("distance", DataTypes.int32))
      val edgeSchema = new EdgeSchema(edgeColumns, "label", "srclabel", "destlabel")
      val edgeRow = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
      val edge = Edge(edgeSchema, edgeRow)
      val addOrientEdge = new EdgeWriter(orientMemoryGraph, edge)
      val oEdge = addOrientEdge.addEdge(oVertexSrc, oVertexDest)
      val srcVidProp: Any = oEdge.getProperty(GraphSchema.srcVidProperty)
      val destVidProp: Any = oEdge.getProperty(GraphSchema.destVidProperty)
      val edgeProp: Any = oEdge.getProperty("distance")
      assert(srcVidProp == 2)
      assert(destVidProp == 3)
      assert(edgeProp == 500)
    }
  }
}
