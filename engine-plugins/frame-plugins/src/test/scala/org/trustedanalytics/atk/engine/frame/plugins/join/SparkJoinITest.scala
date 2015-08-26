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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
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

    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames))

    val resultFrame = JoinRddFunctions.join(RddJoinParam(countryCode, "col_0"),
      RddJoinParam(countryNames, "col_0"), "inner")
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_0", DataTypes.int32, 0),
      Column("col_1_L", DataTypes.str, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, "Iceland")),
      new GenericRow(Array[Any](1, 354, "Ice-land")),
      new GenericRow(Array[Any](2, 91, "India")),
      new GenericRow(Array[Any](2, 100, "India")),
      new GenericRow(Array[Any](3, 47, "Norway")),
      new GenericRow(Array[Any](4, 968, "Oman"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with inner join using broadcast variable" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames))

    val leftJoinParam = RddJoinParam(countryCode, "col_0", Some(150))
    val rightJoinParam = RddJoinParam(countryNames, "col_0", Some(10000))

    val resultFrame = JoinRddFunctions.join(leftJoinParam, rightJoinParam, "inner")
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_0", DataTypes.int32, 0),
      Column("col_1_L", DataTypes.str, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, "Iceland")),
      new GenericRow(Array[Any](1, 354, "Ice-land")),
      new GenericRow(Array[Any](2, 91, "India")),
      new GenericRow(Array[Any](2, 100, "India")),
      new GenericRow(Array[Any](3, 47, "Norway")),
      new GenericRow(Array[Any](4, 968, "Oman"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with left join" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames))

    val resultFrame = JoinRddFunctions.join(RddJoinParam(countryCode, "col_0"),
      RddJoinParam(countryNames, "col_0"), "left")
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_0", DataTypes.int32, 0),
      Column("col_1_L", DataTypes.str, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, "Iceland")),
      new GenericRow(Array[Any](1, 354, "Ice-land")),
      new GenericRow(Array[Any](2, 91, "India")),
      new GenericRow(Array[Any](2, 100, "India")),
      new GenericRow(Array[Any](3, 47, "Norway")),
      new GenericRow(Array[Any](4, 968, "Oman")),
      new GenericRow(Array[Any](5, 50, null))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with left join using broadcast variable" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames))

    val leftJoinParam = RddJoinParam(countryCode, "col_0", Some(1500L))
    val rightJoinParam = RddJoinParam(countryNames, "col_0", Some(100L + Int.MaxValue))

    // Test join wrapper function
    val resultFrame = JoinRddFunctions.join(leftJoinParam, rightJoinParam, "left")
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_0", DataTypes.int32, 0),
      Column("col_1_L", DataTypes.str, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, "Iceland")),
      new GenericRow(Array[Any](1, 354, "Ice-land")),
      new GenericRow(Array[Any](2, 91, "India")),
      new GenericRow(Array[Any](2, 100, "India")),
      new GenericRow(Array[Any](3, 47, "Norway")),
      new GenericRow(Array[Any](4, 968, "Oman")),
      new GenericRow(Array[Any](5, 50, null))
    )

    results should contain theSameElementsAs expectedResults
  }
  "joinRDDs" should "join two RDD with right join" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames))

    val resultFrame = JoinRddFunctions.join(
      RddJoinParam(countryCode, "col_0"),
      RddJoinParam(countryNames, "col_0"), "right")
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_1_L", DataTypes.str, 0),
      Column("col_0", DataTypes.int32, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](354, 1, "Iceland")),
      new GenericRow(Array[Any](354, 1, "Ice-land")),
      new GenericRow(Array[Any](91, 2, "India")),
      new GenericRow(Array[Any](100, 2, "India")),
      new GenericRow(Array[Any](47, 3, "Norway")),
      new GenericRow(Array[Any](968, 4, "Oman")),
      new GenericRow(Array[Any](null, 6, "Germany"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with right join using broadcast variable" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames))

    val broadcastJoinThreshold = 1000
    val leftJoinParam = RddJoinParam(countryCode, "col_0", Some(800))
    val rightJoinParam = RddJoinParam(countryNames, "col_0", Some(4000))

    val resultFrame = JoinRddFunctions.join(leftJoinParam, rightJoinParam, "right", broadcastJoinThreshold)
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_1_L", DataTypes.str, 0),
      Column("col_0", DataTypes.int32, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](354, 1, "Iceland")),
      new GenericRow(Array[Any](354, 1, "Ice-land")),
      new GenericRow(Array[Any](91, 2, "India")),
      new GenericRow(Array[Any](100, 2, "India")),
      new GenericRow(Array[Any](47, 3, "Norway")),
      new GenericRow(Array[Any](968, 4, "Oman")),
      new GenericRow(Array[Any](null, 6, "Germany"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "joinRDDs" should "join two RDD with outer join" in {
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames))

    val resultFrame = JoinRddFunctions.join(RddJoinParam(countryCode, "col_0"),
      RddJoinParam(countryNames, "col_0"), "outer")
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_0", DataTypes.int32, 0),
      Column("col_1_L", DataTypes.str, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, "Iceland")),
      new GenericRow(Array[Any](1, 354, "Ice-land")),
      new GenericRow(Array[Any](2, 91, "India")),
      new GenericRow(Array[Any](2, 100, "India")),
      new GenericRow(Array[Any](3, 47, "Norway")),
      new GenericRow(Array[Any](4, 968, "Oman")),
      new GenericRow(Array[Any](5, 50, null)),
      new GenericRow(Array[Any](6, null, "Germany"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "outer join with empty left RDD" should "preserve the result from the right RDD" in {
    val emptyIdCountryCodes = List.empty[Row]
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(emptyIdCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(idCountryNames))

    val resultFrame = JoinRddFunctions.join(RddJoinParam(countryCode, "col_0"),
      RddJoinParam(countryNames, "col_0"), "outer")
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_0", DataTypes.int32, 0),
      Column("col_1_L", DataTypes.str, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](1, null, "Iceland")),
      new GenericRow(Array[Any](1, null, "Ice-land")),
      new GenericRow(Array[Any](2, null, "India")),
      new GenericRow(Array[Any](3, null, "Norway")),
      new GenericRow(Array[Any](4, null, "Oman")),
      new GenericRow(Array[Any](6, null, "Germany"))
    )

    results should contain theSameElementsAs expectedResults
  }

  "outer join with empty right RDD" should "preserve the result from the left RDD" in {
    val emptyIdCountryNames = List.empty[Row]
    val countryCode = new FrameRdd(codeSchema, sparkContext.parallelize(idCountryCodes))
    val countryNames = new FrameRdd(countrySchema, sparkContext.parallelize(emptyIdCountryNames))

    val resultFrame = JoinRddFunctions.join(RddJoinParam(countryCode, "col_0"),
      RddJoinParam(countryNames, "col_0"), "outer")
    val results = resultFrame.collect()

    resultFrame.frameSchema.columns should equal(List(
      Column("col_0", DataTypes.int32, 0),
      Column("col_1_L", DataTypes.str, 1),
      Column("col_1_R", DataTypes.str, 2)
    ))

    val expectedResults = List(
      new GenericRow(Array[Any](1, 354, null)),
      new GenericRow(Array[Any](2, 91, null)),
      new GenericRow(Array[Any](2, 100, null)),
      new GenericRow(Array[Any](3, 47, null)),
      new GenericRow(Array[Any](4, 968, null)),
      new GenericRow(Array[Any](5, 50, null))
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

    val rddOneToMillion = new FrameRdd(inputSchema, sparkContext.parallelize(oneToOneHundredThousand))
    val rddFiveHundredThousandsToOneFiftyThousands = new FrameRdd(inputSchema,
      sparkContext.parallelize(fiftyThousandToOneFiftyThousands))

    val resultFrame = JoinRddFunctions.join(RddJoinParam(rddOneToMillion, "col_0"),
      RddJoinParam(rddFiveHundredThousandsToOneFiftyThousands, "col_0"), "outer")

    resultFrame.frameSchema.columns should equal(List(
      Column("col_0", DataTypes.int32, 0)
    ))
    resultFrame.count shouldBe 150000
  }

}
