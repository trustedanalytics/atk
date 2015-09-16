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

package org.trustedanalytics.atk.engine.frame.plugins.groupby.aggregators

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.testutils.MatcherUtils._

import org.scalatest.{ Matchers, FlatSpec }

class HistogramAggregatorTest extends FlatSpec with Matchers {
  val epsilon = 0.000001

  "HistogramAggregator" should "return the bin index for the column value based on the cutoffs" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))

    aggregator.mapFunction(null, DataTypes.float64) should equal(-1)
    aggregator.mapFunction(-3, DataTypes.float64) should equal(0)
    aggregator.mapFunction(2.3, DataTypes.float64) should equal(0)
    aggregator.mapFunction(4.5d, DataTypes.float64) should equal(1)
    aggregator.mapFunction(10, DataTypes.float64) should equal(1)
  }

  "HistogramAggregator" should "increment count for the bin corresponding to the bin index" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))

    aggregator.add(Array(1d, 2d, 3d), 2) should equalWithTolerance(Array(1d, 2d, 4d), epsilon)
  }

  "HistogramAggregator" should "not increment count if bin index is out-of-range" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))

    aggregator.add(Array(1d, 2d, 3d), -1) should equalWithTolerance(Array(1d, 2d, 3d), epsilon)
  }

  "HistogramAggregator" should "sum two binned lists" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))

    aggregator.merge(Array(1d, 2d, 3d), Array(4d, 0d, 6d)) should equalWithTolerance(Array(5d, 2d, 9d), epsilon)
  }

  "HistogramAggregator" should "return a vector with the percentage of observations found in each bin" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))
    val histogramDensity = aggregator.getResult(Array(5d, 2d, 9d)).asInstanceOf[Vector[Double]]

    histogramDensity.toArray should equalWithTolerance(Array(5 / 16d, 2 / 16d, 9 / 16d), epsilon)
  }

}
