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
import org.scalatest.{ FlatSpec, Matchers }

class CountAggregatorTest extends FlatSpec with Matchers {
  "CountAggregator" should "output 'one' for each column value" in {
    val aggregator = CountAggregator()

    aggregator.mapFunction("test", DataTypes.string) should equal(1L)
    aggregator.mapFunction(23d, DataTypes.float64) should equal(1L)
  }
  "CountAggregator" should "increment count" in {
    val aggregator = CountAggregator()

    aggregator.add(20L, 1L) should equal(21L)
    aggregator.add(20L, 2L) should equal(22L)
  }
  "CountAggregator" should "sum two counts" in {
    val aggregator = CountAggregator()

    aggregator.merge(5L, 100L) should equal(105L)
  }

}
