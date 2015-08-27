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

package org.trustedanalytics.atk.engine.frame.plugins.cumulativedist

import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class EcdfTest extends TestingSparkContextFlatSpec with Matchers {

  // Input data
  val sampleOneList = List(
    Row(0),
    Row(1),
    Row(2),
    Row(3),
    Row(4),
    Row(5),
    Row(6),
    Row(7),
    Row(8),
    Row(9))

  val sampleTwoList = List(
    Row(0),
    Row(0),
    Row(0),
    Row(0),
    Row(4),
    Row(5),
    Row(6),
    Row(7))

  val sampleThreeList = List(
    Row(-2),
    Row(-1),
    Row(0),
    Row(1),
    Row(2))

  "ecdf" should "compute correct ecdf" in {

    val sampleOneRdd = sparkContext.parallelize(sampleOneList, 2)
    val sampleTwoRdd = sparkContext.parallelize(sampleTwoList, 2)
    val sampleThreeRdd = sparkContext.parallelize(sampleThreeList, 2)
    val colA = Column("a", DataTypes.int32, 0)
    val schema = FrameSchema(List(colA))

    // Get binned results
    val sampleOneECDF = CumulativeDistFunctions.ecdf(new FrameRdd(schema, sampleOneRdd), colA)
    val resultOne = sampleOneECDF.take(10)

    val sampleTwoECDF = CumulativeDistFunctions.ecdf(new FrameRdd(schema, sampleTwoRdd), colA)
    val resultTwo = sampleTwoECDF.take(5)

    val sampleThreeECDF = CumulativeDistFunctions.ecdf(new FrameRdd(schema, sampleThreeRdd), colA)
    val resultThree = sampleThreeECDF.take(5)

    // Validate
    resultOne.apply(0) shouldBe Row(0, 0.1)
    resultOne.apply(1) shouldBe Row(1, 0.2)
    resultOne.apply(2) shouldBe Row(2, 0.3)
    resultOne.apply(3) shouldBe Row(3, 0.4)
    resultOne.apply(4) shouldBe Row(4, 0.5)
    resultOne.apply(5) shouldBe Row(5, 0.6)
    resultOne.apply(6) shouldBe Row(6, 0.7)
    resultOne.apply(7) shouldBe Row(7, 0.8)
    resultOne.apply(8) shouldBe Row(8, 0.9)
    resultOne.apply(9) shouldBe Row(9, 1.0)

    resultTwo.apply(0) shouldBe Row(0, 0.5)
    resultTwo.apply(1) shouldBe Row(4, 0.625)
    resultTwo.apply(2) shouldBe Row(5, 0.75)
    resultTwo.apply(3) shouldBe Row(6, 0.875)
    resultTwo.apply(4) shouldBe Row(7, 1.0)

    resultThree.apply(0) shouldBe Row(-2, 0.2)
    resultThree.apply(1) shouldBe Row(-1, 0.4)
    resultThree.apply(2) shouldBe Row(0, 0.6)
    resultThree.apply(3) shouldBe Row(1, 0.8)
    resultThree.apply(4) shouldBe Row(2, 1.0)
  }

}
