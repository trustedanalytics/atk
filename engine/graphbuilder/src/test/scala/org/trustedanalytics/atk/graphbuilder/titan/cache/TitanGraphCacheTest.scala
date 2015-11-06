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

package org.trustedanalytics.atk.graphbuilder.titan.cache

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.scalatest.{ FlatSpec, Matchers }

class TitanGraphCacheTest extends FlatSpec with Matchers {

  "TitanGraphCache" should "create a Titan graph instance if it does not exist in the cache" in {
    val titanGraphCache = new TitanGraphCache()
    val config = new SerializableBaseConfiguration()
    config.setProperty("storage.backend", "inmemory")

    val titanGraph = titanGraphCache.getGraph(config)

    titanGraph shouldNot equal(null)
    titanGraphCache.cache.size() shouldEqual 1
  }
  "TitanGraphCache" should "retrieve a Titan graph instance if it already exists in the cache" in {
    val titanGraphCache = new TitanGraphCache()
    val config = new SerializableBaseConfiguration()
    config.setProperty("storage.backend", "inmemory")

    val titanGraph = titanGraphCache.getGraph(config)
    val titanGraph2 = titanGraphCache.getGraph(config)

    titanGraph should equal(titanGraph2)
    titanGraphCache.cache.size() shouldEqual 1
  }
  "TitanGraphCache" should "invalidate cache entries" in {
    val titanGraphCache = new TitanGraphCache()
    val config = new SerializableBaseConfiguration()
    config.setProperty("storage.backend", "inmemory")

    val titanGraph = titanGraphCache.getGraph(config)
    titanGraph shouldNot equal(null)
    titanGraphCache.cache.size() shouldEqual 1

    titanGraphCache.invalidateAllCacheEntries()
    titanGraph.isOpen should equal(false)
    titanGraphCache.cache.size() shouldEqual 0

  }
  "TitanGraphCache" should "throw an IllegalArgumentException if hadoop configuration is null" in {
    val titanGraphCache = new TitanGraphCache()
    intercept[IllegalArgumentException] {
      titanGraphCache.getGraph(null)
    }
  }
}
