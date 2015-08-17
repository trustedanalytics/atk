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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.quantiles

import org.trustedanalytics.atk.domain.frame.{ QuantileComposingElement, QuantileTarget }
import org.scalatest.{ FlatSpec, Matchers }

class QuantileTest extends FlatSpec with Matchers {
  "25th quantile" should "be 0.5 * x2 + 0.5 * x3 from 10 elements" in {
    Seq(QuantileComposingElement(2, QuantileTarget(25, 0.5f)), QuantileComposingElement(3, QuantileTarget(25, 0.5f))) shouldBe QuantilesFunctions.getQuantileComposingElements(10, 25)
  }

  "0th quantile" should "be 1 * x1 + 0 * x2 from 5260980 elements" in {
    Seq(QuantileComposingElement(1, QuantileTarget(0, 1))) shouldBe QuantilesFunctions.getQuantileComposingElements(5260980, 0)
  }

  "95th quantile" should "be 1 * x4997931 + 0 * x4997932 from 5260980 elements" in {
    Seq(QuantileComposingElement(4997931, QuantileTarget(95, 1))) shouldBe QuantilesFunctions.getQuantileComposingElements(5260980, 95)
  }

  "99st quantile" should "be 1 * x4997931 + 0 * x4997932 from 5260980 elements" in {
    Seq(QuantileComposingElement(5208370, QuantileTarget(99, BigDecimal(0.8))), QuantileComposingElement(5208371, QuantileTarget(99, BigDecimal(0.2)))) shouldBe QuantilesFunctions.getQuantileComposingElements(5260980, 99)
  }

  "100th quantile" should "be 1 * x1 + 0 * x2 from 5260980 elements" in {
    Seq(QuantileComposingElement(5260980, QuantileTarget(100, 1))) shouldBe QuantilesFunctions.getQuantileComposingElements(5260980, 100)
  }

  "0th, 95th and 99st quantile" should "have mapping for element and mapping" in {
    val mapping = QuantilesFunctions.getQuantileTargetMapping(5260980, Seq(0, 95, 99))

    mapping(1) shouldBe Seq(QuantileTarget(0, 1))
    mapping(4997931) shouldBe Seq(QuantileTarget(95, 1))
    mapping(5208370) shouldBe Seq(QuantileTarget(99, 0.8))
    mapping(5208371) shouldBe Seq(QuantileTarget(99, 0.2))
  }

}
