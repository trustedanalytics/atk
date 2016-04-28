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
package org.apache.spark.atk.graph

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.atk.graph.{ VertexFrameRdd, VertexWrapper }
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema._
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec
import scala.collection.mutable.ArrayBuffer
/**
 * Scala unit test for mapPartitionVertices() method in VertexFrameRdd
 */
class VertexFrameRddTest extends WordSpec with TestingSparkContextWordSpec with Matchers {

  val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
  val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)

  val vertices: List[Row] = List(
    new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350)),
    new GenericRow(Array(2L, "l1", "Alice", "SFO", "SEA", 465)),
    new GenericRow(Array(3L, "l1", "Fred", "NYC", "PIT", 675)),
    new GenericRow(Array(4L, "l1", "Lucy", "LAX", "PDX", 450)))

  "VertexFrameRdd" should {
    "execute user-defined vertex functions in mapPartitionVertices" in {
      val rowRdd = sparkContext.parallelize(vertices)
      val vertexFrameRdd = new VertexFrameRdd(schema, rowRdd)
      val nameRdd = vertexFrameRdd.mapPartitionVertices(iter => {
        val nameBuffer = new ArrayBuffer[String]()

        while (iter.hasNext) {
          val vertexWrapper = iter.next() //Atk vertex wrapper
          val nameProperty = vertexWrapper.stringValue("name")

          nameBuffer += nameProperty
        }
        nameBuffer.toIterator
      })
      //collect the nameRdd into an array
      val nameList = nameRdd.collect()
      nameList should contain theSameElementsAs (Array("Bob", "Alice", "Fred", "Lucy"))
    }
  }

}
