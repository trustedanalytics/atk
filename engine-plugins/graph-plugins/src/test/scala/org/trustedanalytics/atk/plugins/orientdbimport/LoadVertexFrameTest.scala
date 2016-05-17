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

import org.apache.spark.SparkContext
import org.apache.spark.atk.graph.Vertex
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ Matchers, BeforeAndAfterEach, WordSpec }
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.plugins.orientdb.VertexWriter
import org.trustedanalytics.atk.testutils.TestingOrientDb

/**
 * A scala test, imports a vertex class from OrientDB database and returns a list of ATK vertex rows
 */
class LoadVertexFrameTest extends WordSpec with TestingOrientDb with Matchers with BeforeAndAfterEach {
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
    val srcVertex = addOrientVertex.addVertex(vertex)
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "Load vertex frame" should {

    "import OrientDB vertex class as a vertex frame" in {
      val sc = new SparkContext()
      val loadVertexFrame = new LoadVertexFrame(orientMemoryGraph)
      val vertexRowList = loadVertexFrame.importOrientDbVertexClass(sc)
      // vertexRowList(0).toSeq should  contain theSameElementsAs Seq(1L, "_label", "Bob", "PDX", "LAX", 350)
    }
  }
}
