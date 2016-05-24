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

import org.apache.spark.atk.graph.{ Vertex, Edge }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ Matchers, BeforeAndAfterEach, WordSpec }
import org.trustedanalytics.atk.domain.schema.DataTypes.string
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.plugins.orientdb.{ SchemaWriter, EdgeWriter, VertexWriter }
import org.trustedanalytics.atk.testutils.TestingOrientDb

class SchemaReaderTest extends WordSpec with TestingOrientDb with Matchers with BeforeAndAfterEach {

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
    val edge = {
      val edgeColumns = List(Column(GraphSchema.edgeProperty, DataTypes.int64),
        Column(GraphSchema.srcVidProperty, DataTypes.int64),
        Column(GraphSchema.destVidProperty, DataTypes.int64),
        Column(GraphSchema.labelProperty, DataTypes.string),
        Column("distance", DataTypes.int32))
      val edgeSchema = new EdgeSchema(edgeColumns, "label", "srclabel", "destlabel")
      val edgeRow = new GenericRow(Array(1L, 1L, 2L, "distance", 500))
      Edge(edgeSchema, edgeRow)
    }
    val addOrientVertex = new VertexWriter(orientMemoryGraph)
    val schemaWriter = new SchemaWriter(orientMemoryGraph)
    schemaWriter.createVertexSchema(vertex.schema)
    val srcVertex = addOrientVertex.addVertex(vertex)
    val destVertex = addOrientVertex.findOrCreateVertex(2L)
    schemaWriter.createEdgeSchema(edge.schema)
    val edgeWriter = new EdgeWriter(orientMemoryGraph, edge)
    edgeWriter.addEdge(srcVertex, destVertex)
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "Schema reader" should {
    "import vertex schema" in {
      val className = orientMemoryGraph.getVertexType("V").getAllSubclasses.iterator().next().getName
      val vertexSchemaReader = new SchemaReader(orientMemoryGraph)
      // call method under test
      val vertexSchema = vertexSchemaReader.importVertexSchema(className)
      //validate the results
      vertexSchema.columnNames shouldBe List("name", "from", "to", "fair", "_vid", "_label")
      assert(vertexSchema.columnDataType("name") == string)
      assert(vertexSchema.label == "_label")
    }

    "import edge schema" in {
      val className = orientMemoryGraph.getEdgeType("E").getAllSubclasses.iterator().next().getName
      val schemaReader = new SchemaReader(orientMemoryGraph)
      // call method under test
      val edgeSchema = schemaReader.importEdgeSchema(className)
      //validate the results
      edgeSchema.columnNames shouldBe List(GraphSchema.srcVidProperty, "distance", GraphSchema.edgeProperty, GraphSchema.destVidProperty, "_label")
      assert(edgeSchema.columnDataType(GraphSchema.edgeProperty) == DataTypes.int64)
    }
  }

}
