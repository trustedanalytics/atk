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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.descriptives

import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec
import spray.json.DefaultJsonProtocol._
import spray.json._
/**
 * Exercises the categorical summary functions. Primarily checks that correct column indices and options are piped
 * through to the underlying statistics engines. Thorough evaluation of the statistical operations is done by the
 * tests for the respective statistics engines.
 */
class CategoricalSummaryITest extends TestingSparkContextFlatSpec with Matchers {

  trait CategoricalSummaryTest {

    // Input data
    val row1 = ("B", 1)
    val row2 = ("A", 1)
    val row3 = ("B", 1)
    val row4 = ("C", 1)
    val row5 = ("C", 1)
    val row6 = ("C", 1)
    val row7 = ("", 1)
    val row8 = (null, 1)
    val row9 = ("D", 1)
    val row10 = ("D", 1)

    val rowRDD = sparkContext.parallelize(List[(Any, Int)](row1, row2, row3, row4, row5, row6, row7, row8, row9, row10))

  }

  "matchMissingValues" should "return true for null" in {
    val value = (null, 1)
    CategoricalSummaryImpl.matchMissingValues(value) shouldBe true
  }

  "matchMissingValues" should "return false for non null data" in {
    val value = ("valid", 1)
    CategoricalSummaryImpl.matchMissingValues(value) shouldBe false
  }

  "matchMissingValues" should "return true for empty data" in {
    val value = ("", 1)
    CategoricalSummaryImpl.matchMissingValues(value) shouldBe true
  }

  "getTotalCountForSummaryLevels" should "return 10 for total count of all categories" in {
    val levels = List(LevelData("a", 4, 0.4), LevelData("b", 3, 0.3), LevelData("c", 3, 0.3))
    CategoricalSummaryImpl.getTotalCountForSummaryLevels(levels) shouldBe 10
  }

  "getOtherCategoryLevel" should "return LevelData(\"Other\", 5, 0.25)" in {
    implicit val rowCount: Double = 20.0
    val categoricalSummaryLevels = List(LevelData("a", 4, 0.4), LevelData("b", 3, 0.3), LevelData("c", 3, 0.3)): List[LevelData]
    val missingValueCount = 5
    CategoricalSummaryImpl.getOtherCategoryLevel(categoricalSummaryLevels, missingValueCount) shouldBe LevelData("Other", 5, 0.25)
  }

  "getMissingCategoryLevel" should "return LevelData(\"Missing\", 2, 0.2)" in new CategoricalSummaryTest() {
    implicit val rowCount: Double = rowRDD.count()
    CategoricalSummaryImpl.getMissingCategoryLevel(rowRDD) shouldBe LevelData("Missing", 2, 0.2)
  }
}
