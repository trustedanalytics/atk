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
package org.trustedanalytics.atk.plugins.orientdbimport

import org.apache.spark.atk.graph.Vertex
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ Matchers, BeforeAndAfterEach, WordSpec }
import org.trustedanalytics.atk.domain.schema.{ VertexSchema, DataTypes, GraphSchema, Column }
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.trustedanalytics.atk.plugins.TestingOrientDb
import org.trustedanalytics.atk.plugins.orientdb.VertexWriter

class VertexReaderTest extends WordSpec with TestingOrientDb with Matchers with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDbInMemory()
    val vertex = {
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64),
        Column(GraphSchema.labelProperty, DataTypes.string),
        Column("name", DataTypes.string), Column("from", DataTypes.string),
        Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      Vertex(schema, row)
    }
    val addOrientVertex = new VertexWriter(orientMemoryGraph)
    val orientVertex = addOrientVertex.create(vertex)
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "Vertex reader" should {
    val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64),
      Column(GraphSchema.labelProperty, DataTypes.string),
      Column("name", DataTypes.string), Column("from", DataTypes.string),
      Column("to", DataTypes.string), Column("fair", DataTypes.int32))
    val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)

    "import vertex" in {
      val vertexId = 1L
      val orientVertex = orientMemoryGraph.getVertices(GraphSchema.vidProperty, vertexId).iterator().next()
      val vertexReader = new VertexReader(orientMemoryGraph, schema)
      //call method under test
      val atkVertex = vertexReader.importVertex(orientVertex)
      //validate results
      val rowWrapper = new RowWrapper(atkVertex.schema)
      val columnValues = rowWrapper(atkVertex.row).values(atkVertex.schema.columnNames)
      atkVertex.schema.columnNames shouldBe List("_vid", "_label", "name", "from", "to", "fair")
      columnValues shouldBe List(1L, "_label", "Bob", "PDX", "LAX", 350)
    }
  }
}
