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

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.scalatest.{ Matchers, FlatSpec }
import MatcherUtils._

class MeanAggregatorTest extends FlatSpec with Matchers {
  val epsilon = 0.000000001

  "MeanAggregator" should "output column value" in {
    val aggregator = MeanAggregator()

    aggregator.mapFunction(10L, DataTypes.int64) should be(10d +- epsilon)
    aggregator.mapFunction(45d, DataTypes.float64) should be(45d +- epsilon)
    aggregator.mapFunction(0, DataTypes.int64) should be(0d +- epsilon)
    //noinspection NameBooleanParameters,NameBooleanParameters
    aggregator.mapFunction(null, DataTypes.int64).isNaN() should be(true)
  }
  "MeanAggregator" should "throw an IllegalArgumentException if column value is not numeric" in {
    intercept[IllegalArgumentException] {
      val aggregator = MeanAggregator()
      aggregator.mapFunction("test", DataTypes.string)
    }
  }
  "MeanAggregator" should "increment the mean counter" in {
    val aggregator = MeanAggregator()

    aggregator.add(MeanCounter(5, 15d), 4d) should equalWithTolerance(MeanCounter(6, 19d), epsilon)
    aggregator.add(MeanCounter(10, -5d), 4d) should equalWithTolerance(MeanCounter(11, -1d), epsilon)
  }
  "MeanAggregator" should "ignore NaN values in" in {
    val aggregator = MeanAggregator()

    aggregator.add(MeanCounter(5, 15d), Double.NaN) should equalWithTolerance(MeanCounter(5, 15d), epsilon)
    aggregator.add(MeanCounter(10, -5d), 4d) should equalWithTolerance(MeanCounter(11, -1d), epsilon)
  }
  "MeanAggregator" should "merge two mean counters" in {
    val aggregator = MeanAggregator()
    aggregator.merge(MeanCounter(10, -5d), MeanCounter(4, 15d)) should equalWithTolerance(MeanCounter(14, 10d), epsilon)
  }
  "MeanAggregator" should "return mean value" in {
    val aggregator = MeanAggregator()
    aggregator.getResult(MeanCounter(4, 25)).asInstanceOf[Double] should be(6.25d +- epsilon)
  }

}
