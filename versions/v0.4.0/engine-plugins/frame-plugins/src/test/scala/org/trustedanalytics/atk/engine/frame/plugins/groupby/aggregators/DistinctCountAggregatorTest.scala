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
import org.scalatest.{ Matchers, FlatSpec }

class DistinctCountAggregatorTest extends FlatSpec with Matchers {
  "DistinctCountAggregator" should "output column value" in {
    val aggregator = DistinctCountAggregator()

    aggregator.mapFunction("test1", DataTypes.string) should equal("test1")
    aggregator.mapFunction(10L, DataTypes.float64) should equal(10L)

  }
  "DistinctCountAggregator" should "add value to set" in {
    val aggregator = DistinctCountAggregator()
    val set: Set[Any] = Set("test1", "test2", "test3")
    aggregator.add(set, "test4") should contain theSameElementsAs Set("test1", "test2", "test3", "test4")
    aggregator.add(set, "test1") should contain theSameElementsAs Set("test1", "test2", "test3")
  }
  "DistinctCountAggregator" should "merge two sets" in {
    val aggregator = DistinctCountAggregator()

    val set1: Set[Any] = Set("test1", "test2", "test3")
    val set2: Set[Any] = Set(1, 2, 4)
    aggregator.merge(set1, set2) should contain theSameElementsAs Set("test1", "test2", "test3", 1, 2, 4)
    aggregator.merge(Set.empty[Any], set2) should contain theSameElementsAs Set(1, 2, 4)
  }
  "DistinctCountAggregator" should "return count of distinct values" in {
    val aggregator = DistinctCountAggregator()

    val set1: Set[Any] = Set("test1", "test2", "test3")
    aggregator.getResult(set1) should equal(3)
    aggregator.getResult(Set.empty[Any]) should equal(0)
  }

}
