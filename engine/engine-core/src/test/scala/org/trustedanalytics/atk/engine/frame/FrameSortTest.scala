/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.trustedanalytics.atk.engine.frame

import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.{ AnalysisException, Row }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class FrameSortTest extends TestingSparkContextWordSpec with Matchers {
  val inputRows: Array[Row] = Array(
    new GenericRow(Array[Any]("a", 1, 1d, "w")),
    new GenericRow(Array[Any]("c", 5, 1d, "5")),
    new GenericRow(Array[Any]("a", 5, 3d, "z")),
    new GenericRow(Array[Any]("b", -1, 1d, "1")),
    new GenericRow(Array[Any]("a", 2, 1d, "x")),
    new GenericRow(Array[Any]("b", 0, 1d, "2")),
    new GenericRow(Array[Any](null, null, 1d, "6"))
  )

  val inputSchema = FrameSchema(List(
    Column("col_0", DataTypes.string),
    Column("col_1", DataTypes.int32),
    Column("col_2", DataTypes.float64),
    Column("col_3", DataTypes.string)
  ))

  "sortByColumns" should {

    "throw an IllegalArgumentException if sort order is null" in {
      intercept[IllegalArgumentException] {
        val rdd = sparkContext.parallelize(inputRows)
        val frameRdd = new FrameRdd(inputSchema, rdd)
        frameRdd.sortByColumns(null).collect()
      }
    }

    "throw an IllegalArgumentException if sort order is empty" in {
      intercept[IllegalArgumentException] {
        val rdd = sparkContext.parallelize(inputRows)
        val frameRdd = new FrameRdd(inputSchema, rdd)
        frameRdd.sortByColumns(List.empty[(String, Boolean)]).collect()
      }
    }

    "throw an IllegalArgumentException if column names are invalid" in {
      intercept[AnalysisException] {
        val rdd = sparkContext.parallelize(inputRows)
        val frameRdd = new FrameRdd(inputSchema, rdd)
        val sortColumns = List(("invalid_col", true))
        frameRdd.sortByColumns(sortColumns).collect()
      }
    }

    "sort frame by single column in ascending order" in {
      val rdd = sparkContext.parallelize(inputRows)
      val frameRdd = new FrameRdd(inputSchema, rdd)
      val sortColumns = List(("col_3", true))
      val results = frameRdd.sortByColumns(sortColumns).collect()

      val expectedResults = List(
        new GenericRow(Array[Any]("b", -1, 1d, "1")),
        new GenericRow(Array[Any]("b", 0, 1d, "2")),
        new GenericRow(Array[Any]("c", 5, 1d, "5")),
        new GenericRow(Array[Any](null, null, 1d, "6")),
        new GenericRow(Array[Any]("a", 1, 1d, "w")),
        new GenericRow(Array[Any]("a", 2, 1d, "x")),
        new GenericRow(Array[Any]("a", 5, 3d, "z"))
      )

      results.length should equal(7)
      results should contain theSameElementsInOrderAs expectedResults
    }

    "sort frame by single column in descending order" in {
      val rdd = sparkContext.parallelize(inputRows)
      val frameRdd = new FrameRdd(inputSchema, rdd)
      val sortColumns = List(("col_3", false))
      val results = frameRdd.sortByColumns(sortColumns).collect()

      val expectedResults = List(
        new GenericRow(Array[Any]("a", 5, 3d, "z")),
        new GenericRow(Array[Any]("a", 2, 1d, "x")),
        new GenericRow(Array[Any]("a", 1, 1d, "w")),
        new GenericRow(Array[Any](null, null, 1d, "6")),
        new GenericRow(Array[Any]("c", 5, 1d, "5")),
        new GenericRow(Array[Any]("b", 0, 1d, "2")),
        new GenericRow(Array[Any]("b", -1, 1d, "1"))
      )

      results.length should equal(7)
      results should contain theSameElementsInOrderAs expectedResults
    }

    "sort frame by multiple columns" in {
      val rdd = sparkContext.parallelize(inputRows)
      val frameRdd = new FrameRdd(inputSchema, rdd)
      val sortColumns = List(("col_0", true), ("col_1", false))

      val results = frameRdd.sortByColumns(sortColumns).collect()

      val expectedResults = List(
        new GenericRow(Array[Any](null, null, 1d, "6")),
        new GenericRow(Array[Any]("a", 5, 3d, "z")),
        new GenericRow(Array[Any]("a", 2, 1d, "x")),
        new GenericRow(Array[Any]("a", 1, 1d, "w")),
        new GenericRow(Array[Any]("b", 0, 1d, "2")),
        new GenericRow(Array[Any]("b", -1, 1d, "1")),
        new GenericRow(Array[Any]("c", 5, 1d, "5"))
      )

      results.length should equal(7)
      results should contain theSameElementsInOrderAs expectedResults
    }
  }
}

