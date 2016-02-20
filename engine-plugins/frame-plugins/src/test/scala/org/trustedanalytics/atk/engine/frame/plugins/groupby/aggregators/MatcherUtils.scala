/**
 *  Copyright (c) 2016 Intel Corporation 
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

import org.scalatest.Matchers
import org.scalatest.matchers.{ MatchResult, Matcher }

object MatcherUtils extends Matchers {

  /**
   * Tests if two mean counters are equal +- tolerance.
   */
  def equalWithTolerance(right: MeanCounter, tolerance: Double) = Matcher { (left: MeanCounter) =>
    MatchResult(
      left.count == right.count && left.sum === (right.sum +- tolerance),
      left + " did not equal " + right + " with tolerance " + tolerance,
      left + " equaled " + right + " with tolerance " + tolerance
    )
  }

  /**
   * Tests if two mean counters are equal +- tolerance.
   */
  def equalWithTolerance(right: VarianceCounter, tolerance: Double) = Matcher { (left: VarianceCounter) =>
    MatchResult(
      left.count == right.count &&
        left.mean.value === (right.mean.value +- tolerance) &&
        left.mean.delta === (right.mean.delta +- tolerance) &&
        left.m2.value === (right.m2.value +- tolerance) &&
        left.m2.delta === (right.m2.delta +- tolerance),
      left + " did not equal " + right + " with tolerance " + tolerance,
      left + " equaled " + right + " with tolerance " + tolerance
    )
  }
}
