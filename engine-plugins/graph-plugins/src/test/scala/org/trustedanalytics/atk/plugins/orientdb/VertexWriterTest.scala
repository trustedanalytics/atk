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

import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex }
import org.apache.spark.atk.graph.Vertex
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema.{ VertexSchema, DataTypes, GraphSchema, Column }
import org.trustedanalytics.atk.testutils.{ TestingOrientDb, TestingSparkContextWordSpec }

/**
 * scala test for VertexWriter, for add vertex method: checking the exported vertex classname, properties and key index.
 * for findOrCreateVertex method : checking an existing vertex and creates a new vertex if not found
 */
class VertexWriterTest extends WordSpec with Matchers with TestingSparkContextWordSpec with TestingOrientDb with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDbInMemory()
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "Vertex Writer" should {

    "export vertex to OrientDb vertex " in {

      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      val vertex = Vertex(schema, row)
      val addOrientVertex = new VertexWriter(orientMemoryGraph)

      val orientVertex = addOrientVertex.addVertex(vertex)
      val vidProp: Any = orientVertex.getProperty(GraphSchema.vidProperty)
      val propName: Any = orientVertex.getProperty("name")
      val keyIdx = orientMemoryGraph.getIndexedKeys(classOf[BlueprintsVertex])

      assert(propName == "Bob")
      assert(vidProp == 1)
      keyIdx should contain("_vid")
    }
    "findOrCreate gets a vertex" in {
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      val vertex = Vertex(schema, row)
      val addOrientVertex = new VertexWriter(orientMemoryGraph)
      val orientVertex = addOrientVertex.addVertex(vertex)
      val verIdd = orientVertex.getId
      val vertexId = 1L
      val newVertex = addOrientVertex.findOrCreateVertex(vertexId)
      val vidProp: Any = newVertex.getProperty(GraphSchema.vidProperty)
      val propName: Any = newVertex.getProperty("from")
      assert(vidProp == 1)
      assert(propName == "PDX")
    }
    "findOrCreate creates a vertex if not found" in {
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)
      val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
      val vertex = Vertex(schema, row)
      val addOrientVertex = new VertexWriter(orientMemoryGraph)
      val orientVertex = addOrientVertex.addVertex(vertex)
      val verIdd = orientVertex.getId
      val vertexId = 2L
      val newVertex = addOrientVertex.findOrCreateVertex(vertexId)
      val vidProp: Any = newVertex.getProperty(GraphSchema.vidProperty)
      assert(vidProp == 2)

    }
  }

}
