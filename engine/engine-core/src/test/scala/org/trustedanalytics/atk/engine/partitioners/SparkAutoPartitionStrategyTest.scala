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


package org.trustedanalytics.atk.engine.partitioners

import org.trustedanalytics.atk.engine.partitioners.SparkAutoPartitionStrategy._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }

class SparkAutoPartitionStrategyTest extends FlatSpec with Matchers {
  "getPartitionStrategy" should "return the Spark auto-partitioning strategy" in {

    SparkAutoPartitionStrategy.getRepartitionStrategy("disabled") should equal(Disabled)
    SparkAutoPartitionStrategy.getRepartitionStrategy("shrink_only") should equal(ShrinkOnly)
    SparkAutoPartitionStrategy.getRepartitionStrategy("shrink_or_grow") should equal(ShrinkOrGrow)

    SparkAutoPartitionStrategy.getRepartitionStrategy("DISABLED") should equal(Disabled)
    SparkAutoPartitionStrategy.getRepartitionStrategy("SHRINK_ONLY") should equal(ShrinkOnly)
    SparkAutoPartitionStrategy.getRepartitionStrategy("SHRINK_OR_GROW") should equal(ShrinkOrGrow)
  }
}
