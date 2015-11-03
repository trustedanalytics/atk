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

package org.trustedanalytics.atk.engine.frame.plugins.groupby

import org.trustedanalytics.atk.domain.frame.GroupByAggregationArgs
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.{ TestingSparkContextFlatSpec, MatcherUtils }
import MatcherUtils._

import scala.math.BigDecimal.RoundingMode

class GroupByAggregationFunctionsITest extends TestingSparkContextFlatSpec with Matchers {
  val epsilon = 0.000001

  val inputRows: Array[Row] = Array(
    Row("a", 1, 1d, "w"),
    Row("a", 2, 1d, "x"),
    Row("a", 3, 2d, "x"),
    Row("a", 4, 2d, "y"),
    Row("a", 5, 3d, "z"),
    Row("b", -1, 1d, "1"),
    Row("b", 0, 1d, "2"),
    Row("b", 1, 2d, "3"),
    Row("b", 2, null, "4"),
    Row("c", 5, 1d, "5")
  )

  val inputSchema = FrameSchema(List(
    Column("col_0", DataTypes.string),
    Column("col_1", DataTypes.int32),
    Column("col_2", DataTypes.float64),
    Column("col_3", DataTypes.string)
  ))

  "Multi" should "count and sum the number of values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(
      GroupByAggregationArgs("COUNT", "col_1", "col1_count"),
      GroupByAggregationArgs("SUM", "col_2", "col2_sum"),
      GroupByAggregationArgs("SUM", "col_1", "col1_sum"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      Row("a", 5, 9d, 15),
      Row("b", 4, 4d, 2),
      Row("c", 1, 1d, 5)
    )

    results should contain theSameElementsAs expectedResults
  }
  "COUNT" should "count the number of values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("COUNT", "col_1", "col_count"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      Row("a", 5),
      Row("b", 4),
      Row("c", 1)
    )

    results.size shouldBe 3
    results should contain theSameElementsAs expectedResults
  }

  "COUNT_DISTINCT" should "count the number of distinct values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("COUNT_DISTINCT", "col_2", "col_distinct_count"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      Row("a", 3),
      Row("b", 3),
      Row("c", 1)
    )

    results should contain theSameElementsAs expectedResults
  }

  "MIN" should "return the minimum values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("MIN", "col_1", "col_min"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      Row("a", 1),
      Row("b", -1),
      Row("c", 5)
    )

    results should contain theSameElementsAs expectedResults
  }

  "MAX" should "return the maximum values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("MAX", "col_1", "col_max"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      Row("a", 5),
      Row("b", 2),
      Row("c", 5)
    )

    results should contain theSameElementsAs expectedResults
  }

  "SUM" should "return the sum of values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("SUM", "col_1", "col_sum"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect()

    val expectedResults = List(
      Row("a", 15),
      Row("b", 2),
      Row("c", 5)
    )

    results should contain theSameElementsAs expectedResults
  }

  "AVG" should "return the arithmetic mean of values by key" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("AVG", "col_2", "col_mean"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect().map(row => {
      Row(row(0), BigDecimal(row.getDouble(1)).setScale(9, RoundingMode.HALF_UP))
    })

    val expectedResults = List(
      Row("a", BigDecimal(1.8d).setScale(9, RoundingMode.HALF_UP)),
      Row("b", BigDecimal(4 / 3d).setScale(9, RoundingMode.HALF_UP)),
      Row("c", BigDecimal(1d).setScale(9, RoundingMode.HALF_UP))
    )

    results should contain theSameElementsAs expectedResults
  }

  "VAR" should "return the variance of values by key" in {
    val rdd = sparkContext.parallelize(inputRows, 3)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("VAR", "col_2", "col_var"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.collect().map(row => {
      val variance = if (row(1) == null) null else BigDecimal(row.getDouble(1)).setScale(9, RoundingMode.HALF_UP)
      Row(row(0), variance)
    })

    val expectedResults = List(
      Row("a", BigDecimal(0.7d).setScale(9, RoundingMode.HALF_UP)),
      Row("b", BigDecimal(1 / 3d).setScale(9, RoundingMode.HALF_UP)),
      Row("c", null)
    )

    results should contain theSameElementsAs expectedResults
  }

  "HISTOGRAM" should "return the histogram of values by key" in {
    val rdd = sparkContext.parallelize(inputRows, 3)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val groupByColumns = List(inputSchema.column(0))
    val groupByArguments = List(GroupByAggregationArgs("HISTOGRAM={\"cutoffs\": [0,2,4] }", "col_2", "col_histogram"))

    val resultRDD = GroupByAggregationFunctions.aggregation(frameRdd, groupByColumns, groupByArguments)
    val results = resultRDD.map(row => (row(0), row(1).asInstanceOf[Vector[Double]].toArray)).collect().toMap

    results("a") should equalWithTolerance(Array(0.4d, 0.6d), epsilon)
    results("b") should equalWithTolerance(Array(2 / 3d, 1 / 3d), epsilon)
    results("c") should equalWithTolerance(Array(1d, 0d), epsilon)
  }

}
