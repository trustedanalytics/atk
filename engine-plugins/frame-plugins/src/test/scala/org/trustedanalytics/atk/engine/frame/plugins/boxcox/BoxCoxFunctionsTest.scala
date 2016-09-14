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

package org.trustedanalytics.atk.engine.frame.plugins.boxcox

import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.frame.plugins.boxcox.BoxCoxFunctions
import org.trustedanalytics.atk.engine.frame.plugins.dotproduct.DotProductFunctions
import org.trustedanalytics.atk.testutils.{ TestingSparkContextFlatSpec, MatcherUtils }
import MatcherUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers

class BoxCoxFunctionsTest extends TestingSparkContextFlatSpec with Matchers {
  val boxCoxRows: Array[Row] = Array(new GenericRow(Array[Any](7.71320643267, 1.2)),
    new GenericRow(Array[Any](0.207519493594, 2.3)),
    new GenericRow(Array[Any](6.33648234926, 3.4)),
    new GenericRow(Array[Any](7.48803882539, 4.5)),
    new GenericRow(Array[Any](4.98507012303, 5.6)))

  val boxCoxSchema = FrameSchema(List(Column("A", DataTypes.float64), Column("B", DataTypes.float64)))

  val reverseBoxCoxRows: Array[Row] = Array(new GenericRow(Array[Any](2.8191327990706947, 1.2)),
    new GenericRow(Array[Any](-1.253653813751204, 2.3)),
    new GenericRow(Array[Any](2.4667363875200685, 3.4)),
    new GenericRow(Array[Any](2.7646912600285254, 4.5)),
    new GenericRow(Array[Any](2.064011015565803, 5.6)))

  "boxCox" should "compute the box-cox transformation for the given column" in {
    val rdd = sparkContext.parallelize(boxCoxRows)
    val frameRdd = new FrameRdd(boxCoxSchema, rdd)

    val results = BoxCoxFunctions.boxCox(frameRdd, "A", 0.3)
    val boxCox = results.map(row => row(2).asInstanceOf[Double]).collect()
    boxCox should equalWithTolerance(Array(2.81913279907, -1.25365381375, 2.46673638752, 2.76469126003, 2.06401101556))
  }

  "boxCox" should "create a new column in the frame storing the box-cox computation" in {
    val rdd = sparkContext.parallelize(boxCoxRows)
    val frameRdd = new FrameRdd(boxCoxSchema, rdd)
    val result = BoxCoxFunctions.boxCox(frameRdd, "A", 0.3).collect()

    result.length shouldBe 5
    result.apply(0) shouldBe Row(7.71320643267, 1.2, 2.8191327990706947)
    result.apply(1) shouldBe Row(0.207519493594, 2.3, -1.253653813751204)
    result.apply(2) shouldBe Row(6.33648234926, 3.4, 2.4667363875200685)
    result.apply(3) shouldBe Row(7.48803882539, 4.5, 2.7646912600285254)
    result.apply(4) shouldBe Row(4.98507012303, 5.6, 2.064011015565803)
  }

  "reverseBoxCox" should "compute the reverse box-cox transformation for the given column" in {
    val rdd = sparkContext.parallelize(reverseBoxCoxRows)
    val frameRdd = new FrameRdd(boxCoxSchema, rdd)

    val results = BoxCoxFunctions.reverseBoxCox(frameRdd, "A", 0.3)
    val reverseBoxCox = results.map(row => row(2).asInstanceOf[Double]).collect()
    reverseBoxCox should equalWithTolerance(Array(7.713206432669999, 0.20751949359399996, 6.3364823492600015, 7.488038825390002, 4.98507012303))
  }

  "reverseBoxCox" should "create a new column in the frame storing the reverse box-cox computation" in {
    val rdd = sparkContext.parallelize(reverseBoxCoxRows)
    val frameRdd = new FrameRdd(boxCoxSchema, rdd)
    val result = BoxCoxFunctions.reverseBoxCox(frameRdd, "A", 0.3).collect()

    result.length shouldBe 5
    result.apply(0) shouldBe Row(2.8191327990706947, 1.2, 7.713206432669999)
    result.apply(1) shouldBe Row(-1.253653813751204, 2.3, 0.20751949359399996)
    result.apply(2) shouldBe Row(2.4667363875200685, 3.4, 6.3364823492600015)
    result.apply(3) shouldBe Row(2.7646912600285254, 4.5, 7.488038825390002)
    result.apply(4) shouldBe Row(2.064011015565803, 5.6, 4.98507012303)
  }

  "computeDotProduct" should "computeBoxCox transformation" in {
    val input = 4.98507012303
    val lambda = 0.3
    val boxCoxTransform = BoxCoxFunctions.computeBoxCoxTransformation(input, lambda)

    boxCoxTransform shouldBe 2.064011015565803
  }

  "reverseBoxCox" should "computeReverseBoxCox transformation" in {
    val input = 2.8191327990706947
    val lambda = 0.3
    val reverseBoxCoxTransform = BoxCoxFunctions.computeReverseBoxCoxTransformation(input, lambda)

    reverseBoxCoxTransform shouldBe 7.713206432669999
  }

}

