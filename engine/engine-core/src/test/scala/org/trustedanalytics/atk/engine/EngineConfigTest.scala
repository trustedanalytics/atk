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

package org.trustedanalytics.atk.engine

import org.scalatest.FlatSpec

class EngineConfigTest extends FlatSpec {

  "EngineConfig" should "provide the expected maxPartitions size" in {
    assert(EngineConfig.maxPartitions == 10000, "data bricks recommended 10,000 as the max partition value, please don't change unless you are decreasing")
  }

  it should "provide auto partitioner config in the expected order" in {
    val list = EngineConfig.autoPartitionerConfig

    // list should have something in it
    assert(list.size > 3)

    // validate file size is decreasing
    var fileSize = Long.MaxValue
    list.foreach(item => {
      assert(item.fileSizeUpperBound < fileSize)
      fileSize = item.fileSizeUpperBound
    })
  }

  it should "provide auto partitioner config with the expected first element" in {
    val list = EngineConfig.autoPartitionerConfig

    assert(list.size > 3)

    val sixHundredGB = 600000000000L
    assert(list.head.fileSizeUpperBound == sixHundredGB)
    assert(list.head.partitionCount == 7500)
  }

  it should "provide auto partitioner config with the expected last element" in {
    val list = EngineConfig.autoPartitionerConfig

    assert(list.size > 3)
    assert(list.last.fileSizeUpperBound == 1000000)
    assert(list.last.partitionCount == 30)
  }
}
