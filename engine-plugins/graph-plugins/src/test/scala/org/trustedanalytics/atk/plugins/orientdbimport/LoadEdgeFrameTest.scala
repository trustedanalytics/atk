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
import org.apache.spark.atk.graph.Edge
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema.{ EdgeSchema, DataTypes, GraphSchema, Column }
import org.trustedanalytics.atk.plugins.orientdb.{ EdgeWriter, VertexWriter }
import org.trustedanalytics.atk.testutils.TestingOrientDb

/**
 * A scala test, imports a class of edges from OrientDB database and returns a list of ATK edge rows
 */
class LoadEdgeFrameTest extends WordSpec with Matchers with TestingOrientDb with BeforeAndAfterEach {

  override def beforeEach() {
    setupOrientDbInMemory()
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
    val srcVertex = addOrientVertex.findOrCreateVertex(1L)
    val destVertex = addOrientVertex.findOrCreateVertex(2L)
    val edgeWriter = new EdgeWriter(orientMemoryGraph, edge)
    edgeWriter.addEdge(srcVertex, destVertex)
  }

  override def afterEach() {
    cleanupOrientDbInMemory()
  }

  "Load edge frame" should {

    "import OrientDB edge class as a edge frame" in {
      val sc = new SparkContext()
      val loadEdgeFrame = new LoadEdgeFrame(orientMemoryGraph)
      //call method under test
      val edgeRowList = loadEdgeFrame.importOrientDbEdgeClass(sc)
      //validate results
      // edgeRowList(0) shouldBe Row(1L,500,1L, 2L, "label")
    }
  }

}
