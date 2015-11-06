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

class MinAggregatorTest extends FlatSpec with Matchers {
  "MinAggregator" should "output column value" in {
    val aggregator = MinAggregator()

    aggregator.mapFunction("test1", DataTypes.string) should equal("test1")
    aggregator.mapFunction(10L, DataTypes.int64) should equal(10L)

  }
  "MinAggregator" should "return the minimum value in" in {
    val aggregator = MinAggregator()

    aggregator.add(10, 15) should equal(10)
    aggregator.add(-4, -10) should equal(-10)
    aggregator.add(100, 0) should equal(0)
    aggregator.add("test1", "abc") should equal("abc")
  }
  "MinAggregator" should "merge two minimum values" in {
    val aggregator = MinAggregator()

    aggregator.merge(23, 15) should equal(15)
    aggregator.merge(67, -10) should equal(-10)
    aggregator.merge(100, 0) should equal(0)
    aggregator.merge("abc", "def") should equal("abc")
  }
  "MinAggregator" should "ignore null values" in {
    val aggregator = MinAggregator()

    aggregator.add(100, null) should equal(100)
    aggregator.add(null, 10) should equal(10)
    aggregator.merge(-10, null) should equal(-10)
    aggregator.merge(null, 30) should equal(30)
  }

}
