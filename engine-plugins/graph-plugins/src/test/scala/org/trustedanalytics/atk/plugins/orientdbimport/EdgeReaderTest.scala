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

import org.apache.spark.atk.graph.Edge
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ Matchers, BeforeAndAfterEach, WordSpec }
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.trustedanalytics.atk.plugins.orientdb.{ SchemaWriter, VertexWriter, EdgeWriter }
import org.trustedanalytics.atk.testutils.TestingOrientDb

class EdgeReaderTest extends WordSpec with TestingOrientDb with Matchers with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDbInMemory()
    val edge = {
      val edgeColumns = List(Column(GraphSchema.edgeProperty, DataTypes.int64),
        Column(GraphSchema.srcVidProperty, DataTypes.int64),
        Column(GraphSchema.destVidProperty, DataTypes.int64),
        Column(GraphSchema.labelProperty, DataTypes.string),
        Column("distance", DataTypes.int32))
      val edgeSchema = new EdgeSchema(edgeColumns, "label", GraphSchema.labelProperty, GraphSchema.labelProperty)
      val edgeRow = new GenericRow(Array(1L, 1L, 2L, "distance", 500))
      Edge(edgeSchema, edgeRow)
    }
    val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string))
    val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
    val schemaWriter = new SchemaWriter(orientMemoryGraph)
    schemaWriter.createVertexSchema(schema)
    val addOrientVertex = new VertexWriter(orientMemoryGraph)
    val srcVertex = addOrientVertex.findOrCreate(1L, edge.schema.srcVertexLabel)
    val destVertex = addOrientVertex.findOrCreate(2L, edge.schema.destVertexLabel)
    val edgeWriter = new EdgeWriter(orientMemoryGraph, edge)
    edgeWriter.create(srcVertex, destVertex)
  }
  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "Edge reader" should {
    val edgeColumns = List(Column(GraphSchema.edgeProperty, DataTypes.int64),
      Column(GraphSchema.srcVidProperty, DataTypes.int64),
      Column(GraphSchema.destVidProperty, DataTypes.int64),
      Column(GraphSchema.labelProperty, DataTypes.string),
      Column("distance", DataTypes.int32))
    val schema = new EdgeSchema(edgeColumns, "label", GraphSchema.labelProperty, GraphSchema.labelProperty)

    "import edge" in {
      val srcVertexId = 1L
      val orientEdge = orientMemoryGraph.getEdges(GraphSchema.srcVidProperty, srcVertexId).iterator().next()
      val edgeReader = new EdgeReader(orientMemoryGraph, schema)
      //method under test
      val atkEdge = edgeReader.importEdge(orientEdge)
      //validate results
      val rowWrapper = new RowWrapper(atkEdge.schema)
      rowWrapper(atkEdge.row).valuesAsArray(schema.columnNames) shouldBe Array(1L, 1L, 2L, "label", 500)
    }
  }

}
