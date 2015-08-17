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

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION
import org.apache.hadoop.conf.Configuration
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }

class TitanHadoopCacheConfigurationTest extends FlatSpec with Matchers with BeforeAndAfter {
  val hadoopConfig = new Configuration()
  val faunusConf = null

  "TitanHadoopCacheConfiguration" should "create a TitanHadoopCacheConfiguration" in {
    val hadoopConfig = new Configuration()
    hadoopConfig.set("titan.hadoop.input.conf.storage.backend", "inmemory")

    val faunusConf = new ModifiableHadoopConfiguration(hadoopConfig)
    faunusConf.set(TITAN_INPUT_VERSION, "current")

    val titanHadoopCacheConf = new TitanHadoopCacheConfiguration(faunusConf)

    titanHadoopCacheConf.getFaunusConfiguration() should equal(faunusConf)
    titanHadoopCacheConf.getInputFormatClassName() should equal("com.thinkaurelius.titan.hadoop.formats.util.input.current.TitanHadoopSetupImpl")
  }

  "TitanHadoopCacheConfiguration" should "equal a TitanHadoopCacheConfiguration with the same Titan properties" in {
    val hadoopConfig = new Configuration()
    hadoopConfig.set("titan.hadoop.input.conf.storage.backend", "inmemory")
    hadoopConfig.set("titan.hadoop.input.conf.storage.batch-loading", "true")

    val faunusConf = new ModifiableHadoopConfiguration(hadoopConfig)
    faunusConf.set(TITAN_INPUT_VERSION, "current")

    val titanHadoopCacheConf = new TitanHadoopCacheConfiguration(faunusConf)
    val titanHadoopCacheConf2 = new TitanHadoopCacheConfiguration(faunusConf)

    titanHadoopCacheConf should equal(titanHadoopCacheConf2)
    titanHadoopCacheConf.hashCode() should equal(titanHadoopCacheConf2.hashCode())
  }

  "TitanHadoopCacheConfiguration" should "not equal a TitanHadoopCacheConfiguration with different Titan properties" in {
    val hadoopConfig = new Configuration()
    hadoopConfig.set("titan.hadoop.input.conf.storage.backend", "inmemory")
    hadoopConfig.set("titan.hadoop.input.conf.storage.batch-loading", "true")
    val faunusConf = new ModifiableHadoopConfiguration(hadoopConfig)
    faunusConf.set(TITAN_INPUT_VERSION, "current")

    val titanHadoopCacheConf = new TitanHadoopCacheConfiguration(faunusConf)

    val hadoopConfig2 = new Configuration()
    hadoopConfig2.set("titan.hadoop.input.conf.storage.backend", "hbase")
    hadoopConfig2.set("titan.hadoop.input.conf.storage.batch-loading", "false")
    val faunusConf2 = new ModifiableHadoopConfiguration(hadoopConfig2)
    faunusConf2.set(TITAN_INPUT_VERSION, "current")

    val titanHadoopCacheConf2 = new TitanHadoopCacheConfiguration(faunusConf2)

    titanHadoopCacheConf should not equal titanHadoopCacheConf2
    titanHadoopCacheConf.hashCode() should not equal titanHadoopCacheConf2.hashCode()
  }
  "TitanHadoopCacheConfiguration" should "throw an IllegalArgumentException if Faunus configuration is null" in {
    intercept[IllegalArgumentException] {
      val titanHadoopCacheConf = new TitanHadoopCacheConfiguration(null)
    }
  }
}
