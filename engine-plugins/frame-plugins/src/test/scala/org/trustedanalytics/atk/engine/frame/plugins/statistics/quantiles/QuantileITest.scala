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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.quantiles

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class QuantileITest extends TestingSparkContextFlatSpec with Matchers {
  "Calculation quantile in small data set" should "return the correct values" in {
    val numbers = List(Array[Any](3, ""), Array[Any](5, ""),
      Array[Any](6, ""), Array[Any](7, ""), Array[Any](23, ""), Array[Any](8, ""), Array[Any](21, ""), Array[Any](9, ""), Array[Any](11, ""),
      Array[Any](20, ""), Array[Any](13, ""), Array[Any](15, ""), Array[Any](10, ""), Array[Any](16, ""), Array[Any](17, ""),
      Array[Any](18, ""), Array[Any](1, ""), Array[Any](19, ""), Array[Any](4, ""), Array[Any](22, ""),
      Array[Any](24, ""), Array[Any](12, ""), Array[Any](2, ""), Array[Any](14, ""), Array[Any](25, "")
    )

    val rdd: RDD[Row] = sparkContext.parallelize(numbers.map(a => new GenericRow(a)), 3)
    val result = QuantilesFunctions.quantiles(rdd, Seq(0, 3, 5, 40, 100), 0, numbers.size.toLong).collect()
    result.size shouldBe 5
    result(0) shouldBe Row(0.0, 1.0)
    result(1) shouldBe Row(3.0, 1.0)
    result(2) shouldBe Row(5.0, 1.25)
    result(3) shouldBe Row(40.0, 10.0)
    result(4) shouldBe Row(100.0, 25.0)
  }

  //   Large scale test takes longer time. uncomment it when needed.
  //  "Calculation quantile in large data set" should "return the correct values" in {
  //
  //    import scala.util.Random
  //    val numbers = ListBuffer[Array[Any]]()
  //    numbers
  //    for (i <- 1 to 1000000) {
  //      numbers += Array[Any](i, "")
  //    }
  //
  //    val randomPositionedNumbers = Random.shuffle(numbers)
  //
  //    val rdd = sc.parallelize(randomPositionedNumbers, 90)
  //    val result = SparkOps.calculatePercentiles(rdd, Seq(5, 40), 0, DataTypes.int32)
  //    result.length shouldBe 2
  //    result(0) shouldBe(5, 50000)
  //    result(1) shouldBe(40, 400000)
  //  }
}
