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


package org.trustedanalytics.atk.engine.frame.plugins.groupby.aggregators

import org.trustedanalytics.atk.domain.frame.GroupByAggregationArgs
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column }
import org.scalatest.{ Matchers, FlatSpec }

class ColumnAggregatorTest extends FlatSpec with Matchers {

  "getHistogramColumnAggregator" should "return the column aggregator for histogram" in {
    val groupByArguments = GroupByAggregationArgs("HISTOGRAM={\"cutoffs\": [0,2,4] }", "col_2", "col_histogram")

    val columnAggregator = ColumnAggregator.getHistogramColumnAggregator(groupByArguments, 5)
    val expectedResults = ColumnAggregator(Column("col_histogram", DataTypes.vector(2)), 5, HistogramAggregator(List(0d, 2d, 4d)))

    columnAggregator should equal(expectedResults)
  }

  "getHistogramColumnAggregator" should "throw an IllegalArgumentException if cutoffs are missing" in {
    val groupByArguments = GroupByAggregationArgs("HISTOGRAM", "col_2", "col_histogram")

    intercept[IllegalArgumentException] {
      ColumnAggregator.getHistogramColumnAggregator(groupByArguments, 5)
    }
  }

  "getHistogramColumnAggregator" should "throw an IllegalArgumentException if cutoffs are not numeric" in {
    val groupByArguments = GroupByAggregationArgs("HISTOGRAM={\"cutoffs\": [\"a\",\"b\"] }", "col_2", "col_histogram")

    intercept[IllegalArgumentException] {
      ColumnAggregator.getHistogramColumnAggregator(groupByArguments, 5)
    }
  }

  "parseHistogramCutoffs" should "return the cutoffs" in {
    val histogramAggregator = ColumnAggregator.getHistogramAggregator("{\"cutoffs\": [4,5,6] }")
    histogramAggregator.cutoffs should equal(List(4d, 5d, 6d))
  }

  "parseHistogramCutoffs" should "throw an IllegalArgumentException if cutoffs are not numeric" in {
    intercept[IllegalArgumentException] {
      ColumnAggregator.getHistogramAggregator("{\"cutoffs\": [\"a\",\"b\"] }")
    }
  }

  "parseHistogramCutoffs" should "throw an IllegalArgumentException if cutoffs are missing" in {
    intercept[IllegalArgumentException] {
      ColumnAggregator.getHistogramAggregator("{\"missing\": [4,5,6] }")
    }
  }

  "parseHistogramCutoffs" should "throw an IllegalArgumentException if cutoffs are not valid json" in {
    intercept[IllegalArgumentException] {
      ColumnAggregator.getHistogramAggregator("=={\"missing\": [4,5,6] }")
    }
  }
}
