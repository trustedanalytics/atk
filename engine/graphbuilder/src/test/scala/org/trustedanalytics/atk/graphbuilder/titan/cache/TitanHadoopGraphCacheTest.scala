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

package org.trustedanalytics.atk.graphbuilder.titan.cache

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration
import org.apache.hadoop.conf.Configuration
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }

class TitanHadoopGraphCacheTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfter {
  val hadoopCacheConfiguration = mock[TitanHadoopCacheConfiguration]
  val hadoopConfig = new Configuration()

  before {
    hadoopConfig.set("titan.hadoop.input.conf.storage.backend", "inmemory")
    when(hadoopCacheConfiguration.getInputFormatClassName).thenReturn("com.thinkaurelius.titan.hadoop.formats.util.input.current.TitanHadoopSetupImpl")
    when(hadoopCacheConfiguration.getFaunusConfiguration).thenReturn(new ModifiableHadoopConfiguration(hadoopConfig))
  }

  "TitanHadoopGraphCache" should "create a Titan/Hadoop graph instance if it does not exist in the cache" in {
    val titanHadoopGraphCache = new TitanHadoopGraphCache()
    val config = new SerializableBaseConfiguration()
    config.setProperty("storage.backend", "inmemory")

    val titanHadoopGraph = titanHadoopGraphCache.getGraph(hadoopCacheConfiguration)

    titanHadoopGraph shouldNot equal(null)
    titanHadoopGraphCache.cache.size() shouldEqual 1
  }
  "TitanHadoopGraphCache" should "retrieve a Titan/Hadoop graph instance if it already exists in the cache" in {
    val titanHadoopGraphCache = new TitanHadoopGraphCache()

    val titanHadoopGraph = titanHadoopGraphCache.getGraph(hadoopCacheConfiguration)
    val titanHadoopGraph2 = titanHadoopGraphCache.getGraph(hadoopCacheConfiguration)

    titanHadoopGraph should equal(titanHadoopGraph2)
    titanHadoopGraphCache.cache.size() shouldEqual 1
  }
  "TitanHadoopGraphCache" should "invalidate cache entries" in {
    val titanHadoopGraphCache = new TitanHadoopGraphCache()

    val titanHadoopGraph = titanHadoopGraphCache.getGraph(hadoopCacheConfiguration)
    titanHadoopGraph shouldNot equal(null)
    titanHadoopGraphCache.cache.size() shouldEqual 1

    titanHadoopGraphCache.invalidateAllCacheEntries()
    titanHadoopGraphCache.cache.size() shouldEqual 0

  }
  "TitanHadoopGraphCache" should "throw an IllegalArgumentException if hadoop configuration is null" in {
    val titanHadoopGraphCache = new TitanHadoopGraphCache()
    intercept[IllegalArgumentException] {
      titanHadoopGraphCache.getGraph(null)
    }
  }

}
