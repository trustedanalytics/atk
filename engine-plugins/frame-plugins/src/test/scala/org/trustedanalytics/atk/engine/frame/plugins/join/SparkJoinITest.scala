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

package org.trustedanalytics.atk.engine.frame.plugins.join

import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class SparkJoinITest extends TestingSparkContextFlatSpec with Matchers {
  // Test data has duplicate keys, matching and non-matching keys
  val idCountryCodes: List[Row] = List(
    new GenericRow(Array[Any](1, 354)),
    new GenericRow(Array[Any](2, 91)),
    new GenericRow(Array[Any](2, 100)),
    new GenericRow(Array[Any](3, 47)),
    new GenericRow(Array[Any](4, 968)),
    new GenericRow(Array[Any](5, 50)))

  val idCountryNames: List[Row] = List(
    new GenericRow(Array[Any](1, "Iceland")),
    new GenericRow(Array[Any](1, "Ice-land")),
    new GenericRow(Array[Any](2, "India")),
    new GenericRow(Array[Any](3, "Norway")),
    new GenericRow(Array[Any](4, "Oman")),
    new GenericRow(Array[Any](6, "Germany"))
  )

  val codeSchema = FrameSchema(List(
    Column("col_0", DataTypes.int32),
    Column("col_1", DataTypes.str)
  ))

  val countrySchema = FrameSchema(List(
    Column("col_0", DataTypes.int32),
    Column("col_1", DataTypes.str)
  ))

  "joinRDDs" should "join two RDD with inner join" in {

    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames)).toDataFrame

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, "col_0", 0, 2),
      RddJoinParam(countryNames, "col_0", 0, 2), "inner").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with inner join using broadcast variable" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames)).toDataFrame

    val leftJoinParam = RddJoinParam(countryCode, "col_0", 0, 2, Some(150))
    val rightJoinParam = RddJoinParam(countryNames, "col_0", 0, 2, Some(10000))

    val results = JoinRddFunctions.joinRDDs(leftJoinParam, rightJoinParam, "inner").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with left join" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames)).toDataFrame

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, "col_0", 0, 2),
      RddJoinParam(countryNames, "col_0", 0, 2), "left").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](5, 50, null, null))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with left join using broadcast variable" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames)).toDataFrame

    val leftJoinParam = RddJoinParam(countryCode, "col_0", 0, 2, Some(1500L))
    val rightJoinParam = RddJoinParam(countryNames, "col_0", 0, 2, Some(100L + Int.MaxValue))

    // Test join wrapper function
    val results = JoinRddFunctions.joinRDDs(leftJoinParam, rightJoinParam, "left").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](5, 50, null, null))
    )

    results should contain theSameElementsAs expectedResults
  }
  "joinRDDs" should "join two RDD with right join" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames)).toDataFrame

    val results = JoinRddFunctions.joinRDDs(
      RddJoinParam(countryCode, "col_0", 0, 2),
      RddJoinParam(countryNames, "col_0", 0, 2), "right").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](null, null, 6, "Germany"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with right join using broadcast variable" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames)).toDataFrame

    val broadcastJoinThreshold = 1000
    val leftJoinParam = RddJoinParam(countryCode, "col_0", 0, 2, Some(800))
    val rightJoinParam = RddJoinParam(countryNames, "col_0", 0, 2, Some(4000))

    val results = JoinRddFunctions.joinRDDs(leftJoinParam, rightJoinParam, "right", broadcastJoinThreshold).collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](null, null, 6, "Germany"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with outer join" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames)).toDataFrame

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, "col_0", 0, 2),
      RddJoinParam(countryNames, "col_0", 0, 2), "outer").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, 1, "Iceland")),
      new GenericRow(Array[Any](1, 354, 1, "Ice-land")),
      new GenericRow(Array[Any](2, 91, 2, "India")),
      new GenericRow(Array[Any](2, 100, 2, "India")),
      new GenericRow(Array[Any](3, 47, 3, "Norway")),
      new GenericRow(Array[Any](4, 968, 4, "Oman")),
      new GenericRow(Array[Any](5, 50, null, null)),
      new GenericRow(Array[Any](null, null, 6, "Germany"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "outer join with empty left RDD" should "preserve the result from the right RDD" in {
    val emptyIdCountryCodes = List.empty[Row]
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(emptyIdCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames)).toDataFrame

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, "col_0", 0, 2),
      RddJoinParam(countryNames, "col_0", 0, 2), "outer").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](null, null, 1, "Iceland")),
      new GenericRow(Array[Any](null, null, 1, "Ice-land")),
      new GenericRow(Array[Any](null, null, 2, "India")),
      new GenericRow(Array[Any](null, null, 3, "Norway")),
      new GenericRow(Array[Any](null, null, 4, "Oman")),
      new GenericRow(Array[Any](null, null, 6, "Germany"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "outer join with empty right RDD" should "preserve the result from the left RDD" in {
    val emptyIdCountryNames = List.empty[Row]
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes)).toDataFrame
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(emptyIdCountryNames)).toDataFrame

    val results = JoinRddFunctions.joinRDDs(RddJoinParam(countryCode, "col_0", 0, 2),
      RddJoinParam(countryNames, "col_0", 0, 2), "outer").collect()

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, null, null)),
      new GenericRow(Array[Any](2, 91, null, null)),
      new GenericRow(Array[Any](2, 100, null, null)),
      new GenericRow(Array[Any](3, 47, null, null)),
      new GenericRow(Array[Any](4, 968, null, null)),
      new GenericRow(Array[Any](5, 50, null, null))
    )

    results should contain theSameElementsAs expectedResults
  }

  "outer join large RDD" should "generate RDD contains all element from both RDD" in {
    val oneToOneHundredThousand: List[Row] = (1 to 100000).map(i => {
      new GenericRow(Array[Any](i))
    }).toList

    val fiftyThousandToOneFiftyThousands: List[Row] = (50001 to 150000).map(i => {
      new GenericRow(Array[Any](i))
    }).toList

    val inputSchema = FrameSchema(List(Column("col_0", DataTypes.int32)))

    val rddOneToMillion = new FrameRdd(inputSchema, sparkContext.parallelize(oneToOneHundredThousand)).toDataFrame
    val rddFiveHundredThousandsToOneFiftyThousands = new FrameRdd(inputSchema,
      sparkContext.parallelize(fiftyThousandToOneFiftyThousands)).toDataFrame

    val rddFullOuterJoin = JoinRddFunctions.joinRDDs(RddJoinParam(rddOneToMillion, "col_0", 0, 1),
      RddJoinParam(rddFiveHundredThousandsToOneFiftyThousands, "col_0", 0, 1), "outer")
    rddFullOuterJoin.count shouldBe 150000
  }

}
