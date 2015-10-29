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


package org.trustedanalytics.atk.graphbuilder.graph.titan

import org.apache.commons.configuration.BaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{ ClusterStatus, HBaseConfiguration, HRegionInfo, TableName }
import org.apache.spark.{ SparkConf, SparkContext }
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }

class TitanAutoPartitionerITest extends FlatSpec with Matchers with MockitoSugar {

  val hBaseTableName = "testtable"
  val hBaseRegionServers = 3
  val hBaseTableRegions = 5
  val hBaseTableSizeMB = 2000

  val hBaseAdminMock = mock[HBaseAdmin]
  val clusterStatusMock = mock[ClusterStatus]
  val tableRegionsMock = mock[java.util.List[HRegionInfo]]

  val hBaseConfig = HBaseConfiguration.create()

  when(hBaseAdminMock.getConfiguration).thenReturn(hBaseConfig)

  //Mock number of HBase region servers
  when(hBaseAdminMock.getClusterStatus()).thenReturn(clusterStatusMock)
  when(hBaseAdminMock.getClusterStatus.getServersSize).thenReturn(hBaseRegionServers)

  //Mock number of regions in HBase table
  when(hBaseAdminMock.getTableRegions(TableName.valueOf(hBaseTableName))).thenReturn(tableRegionsMock)
  when(hBaseAdminMock.getTableRegions(TableName.valueOf(hBaseTableName)).size()).thenReturn(hBaseTableRegions)

  "enableAutoPartition" should "return true when auto-partitioner is enabled" in {
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")

    val titanAutoPartitioner = new TitanAutoPartitioner(titanConfig)
    titanAutoPartitioner.enableAutoPartition shouldBe true
  }

  "enableAutoPartition" should "return false when auto-partitioner is disabled" in {
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "false")

    val titanAutoPartitioner = new TitanAutoPartitioner(titanConfig)
    titanAutoPartitioner.enableAutoPartition shouldBe false
  }

  "setHBasePreSplits" should "set HBase pre-splits for graph construction based on available region servers" in {
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")
    titanConfig.setProperty(TitanAutoPartitioner.HBASE_REGIONS_PER_SERVER, "6")

    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    val newTitanConfig = titanAutoPartitioner.setHBasePreSplits(hBaseAdminMock)

    val expectedRegionCount = hBaseRegionServers * 6
    newTitanConfig.getProperty(TitanAutoPartitioner.TITAN_HBASE_REGION_COUNT) shouldBe expectedRegionCount
  }
  "setHBasePreSplits" should "not set HBase pre-splits if regions are less than Titan's minimum region count" in {
    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")
    titanConfig.setProperty(TitanAutoPartitioner.HBASE_REGIONS_PER_SERVER, "0")

    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    val newTitanConfig = titanAutoPartitioner.setHBasePreSplits(hBaseAdminMock)
    newTitanConfig.getProperty(TitanAutoPartitioner.TITAN_HBASE_REGION_COUNT) shouldBe null
  }
  "getHBaseInputSplits" should "get input splits for Titan/HBase reader using spark.cores.max" in {
    val sparkConfig = new SparkConf()
    val sparkContextMock = mock[SparkContext]
    val tableName = TableName.valueOf(hBaseTableName)
    val hBaseConfig = hBaseAdminMock.getConfiguration

    sparkConfig.set(TitanAutoPartitioner.SPARK_MAX_CORES, "20")
    when(sparkContextMock.getConf).thenReturn(sparkConfig)

    val titanConfig = new BaseConfiguration()
    titanConfig.setProperty(TitanAutoPartitioner.ENABLE_AUTO_PARTITION, "true")
    titanConfig.setProperty(TitanAutoPartitioner.HBASE_INPUT_SPLITS_PER_CORE, 2)

    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    val inputSplits = titanAutoPartitioner.getSparkHBaseInputSplits(sparkContextMock, hBaseAdminMock, hBaseTableName)

    val expectedHBaseSplits = Math.round(Math.log1p(20) / Math.log(2)) * 2
    inputSplits shouldBe expectedHBaseSplits
  }

}
