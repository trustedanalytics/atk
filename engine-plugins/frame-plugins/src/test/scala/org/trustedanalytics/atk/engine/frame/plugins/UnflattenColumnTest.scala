/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.frame.plugins

import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, DataTypes }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.scalatest.{ BeforeAndAfterEach, FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class UnflattenColumnTest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  private val nameColumn = "name"
  private val dateColumn = "date"

  val dailyHeartbeats_4_1 = List(
    Array[Any]("Bob", "1/1/2015", "1", "60"),
    Array[Any]("Bob", "1/1/2015", "2", "70"),
    Array[Any]("Bob", "1/1/2015", "3", "65"),
    Array[Any]("Bob", "1/1/2015", "4", "55"))

  val dailyHeartbeats_1_1 = List(
    Array[Any]("Bob", "1/1/2015", "1", "60"))

  val dailyHeartbeats_2_2 = List(
    Array[Any]("Mary", "1/1/2015", "1", "60"),
    Array[Any]("Bob", "1/1/2015", "1", "60"))

  def executeTest(data: List[Array[Any]], rowsInResult: Int): Array[Row] = {
    val schema = FrameSchema(List(Column(nameColumn, DataTypes.string),
      Column(dateColumn, DataTypes.string),
      Column("minute", DataTypes.int32),
      Column("heartRate", DataTypes.int32)))
    val compositeKeyColumnNames = List(nameColumn, dateColumn)
    val compositeKeyIndices = List(0, 1)

    val rows = sparkContext.parallelize(data)
    val rdd = FrameRdd.toFrameRdd(schema, rows).groupByRows(row => row.values(compositeKeyColumnNames))

    val targetSchema = UnflattenColumnFunctions.createTargetSchema(schema, compositeKeyColumnNames)
    val resultRdd = UnflattenColumnFunctions.unflattenRddByCompositeKey(compositeKeyIndices, rdd, targetSchema, ",")

    resultRdd.take(rowsInResult)
  }

  "UnflattenRddByCompositeKey::1" should "compress data in a single row" in {

    val rowInResult = 1
    val result = executeTest(dailyHeartbeats_4_1, rowInResult)

    assert(result.length == rowInResult)
    result.apply(rowInResult - 1) shouldBe Row("Bob", "1/1/2015", "1,2,3,4", "60,70,65,55")
  }

  "UnflattenRddByCompositeKey::2" should "compress data in a single row" in {

    val rowInResult = 1
    val result = executeTest(dailyHeartbeats_1_1, rowInResult)

    assert(result.length == rowInResult)
    result.apply(rowInResult - 1) shouldBe Row("Bob", "1/1/2015", "1", "60")
  }

  "UnflattenRddByCompositeKey::3" should "compress data in two rows" in {
    val rowInResult = 2
    val result = executeTest(dailyHeartbeats_2_2, rowInResult)

    assert(result.length == rowInResult)
  }
}
