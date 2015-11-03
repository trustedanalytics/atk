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

import com.google.common.annotations.VisibleForTesting
import org.trustedanalytics.atk.graphbuilder.titan.io.GBTitanHBaseInputFormat
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Auto-partitioner for Titan graphs that is used to increase concurrency of reads and writes to Titan storage backends.
 *
 * @param titanConfig Titan configuration
 */
case class TitanAutoPartitioner(titanConfig: Configuration) {

  import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanAutoPartitioner._

  val enableAutoPartition = titanConfig.getBoolean(ENABLE_AUTO_PARTITION, false)

  /**
   * Set the number of HBase pre-splits in the Titan configuration
   *
   * Updates the Titan configuration option for region count, if the auto-partitioner is enabled,
   * and the pre-split count exceeds the minimum region count allowed by Titan.
   *
   * @param hBaseAdmin HBase administration
   * @return Updated Titan configuration
   */
  def setHBasePreSplits(hBaseAdmin: HBaseAdmin): Configuration = {
    if (enableAutoPartition) {
      val regionCount = getHBasePreSplits(hBaseAdmin)
      if (regionCount >= HBaseStoreManager.MIN_REGION_COUNT) {
        titanConfig.setProperty(TITAN_HBASE_REGION_COUNT, regionCount)
      }
    }
    titanConfig
  }

  /**
   * Sets the HBase input splits for the HBase table input format for Spark.
   *
   * @param sparkContext Spark context
   * @param hBaseAdmin HBase administration
   * @param titanGraphName  Titan graph name
   *
   * @return Updated HBase configuration
   */
  def setSparkHBaseInputSplits(sparkContext: SparkContext,
                               hBaseAdmin: HBaseAdmin,
                               titanGraphName: String): org.apache.hadoop.conf.Configuration = {
    val hBaseConfig = hBaseAdmin.getConfiguration
    setSparkHBaseInputSplits(sparkContext, hBaseConfig, titanGraphName)
    hBaseConfig
  }

  /**
   * Sets the HBase input splits for the HBase table input format in the HBase configuration
   * that is supplied as input for Spark.
   *
   * @param sparkContext Spark context
   * @param hBaseConfig HBase configuration
   * @param titanGraphName  Titan graph name
   */
  def setSparkHBaseInputSplits(sparkContext: SparkContext,
                               hBaseConfig: org.apache.hadoop.conf.Configuration,
                               titanGraphName: String): Unit = {
    if (enableAutoPartition) {
      val tableName = TableName.valueOf(titanGraphName)

      val hBaseAdmin = new HBaseAdmin(hBaseConfig)
      val regionCount = Math.max(1, Try(hBaseAdmin.getTableRegions(tableName).size()).getOrElse(0))

      val inputSplits = getSparkHBaseInputSplits(sparkContext, hBaseAdmin, titanGraphName)
      if (inputSplits > regionCount) {
        hBaseConfig.setInt(GBTitanHBaseInputFormat.NUM_REGION_SPLITS, inputSplits)
      }
      println("Region count: " + regionCount + ", input splits: " + inputSplits)
    }
  }

  /**
   * Sets the HBase input splits for the HBase table input format in the HBase configuration
   * that is supplied as input for Giraph.
   *
   * @param hBaseConfig HBase configuration
   * @param numGiraphWorkers Number of Giraph workers
   */
  def setGiraphHBaseInputSplits(hBaseConfig: org.apache.hadoop.conf.Configuration, numGiraphWorkers: Int): Unit = {
    if (enableAutoPartition && numGiraphWorkers > 1) {
      hBaseConfig.setInt(GBTitanHBaseInputFormat.NUM_REGION_SPLITS, numGiraphWorkers)
    }
  }

  /**
   * Get input splits for Titan/HBase reader for Spark.
   *
   * The default input split policy for HBase tables is one Spark partition per HBase region. This
   * function computes the desired number of HBase input splits based on the number of available Spark
   * cores in the cluster, the user-defined configuration for splits/core, and the graph size.
   *
   * Desired input splits = input-splits-per-spark-core * log(available spark-cores) * log(graph size in HBase in MB),
   *
   * @param sparkContext Spark context
   * @param hBaseAdmin HBase administration
   * @param titanGraphName Titan graph name
   * @return Desired number of HBase input splits
   */
  def getSparkHBaseInputSplits(sparkContext: SparkContext,
                               hBaseAdmin: HBaseAdmin,
                               titanGraphName: String): Int = {
    val tableName = TableName.valueOf(titanGraphName)

    // Set minimum values to 1
    val regionCount = Math.max(1, Try(hBaseAdmin.getTableRegions(tableName).size()).getOrElse(0))
    val tableSizeMb = Math.max(1, getTableSizeInMb(hBaseAdmin.getConfiguration, tableName))
    val TableSizeMbLog2 = Math.round(Math.log1p(tableSizeMb) / Math.log(2))

    val maxSparkCores = Math.max(1, getMaxSparkCores(sparkContext, hBaseAdmin))
    val maxSparkCoresLog2 = Math.round(Math.log1p(maxSparkCores) / Math.log(2))
    val splitsPerCore = titanConfig.getInt(HBASE_INPUT_SPLITS_PER_CORE, 1)

    Math.max((TableSizeMbLog2 * maxSparkCoresLog2 * splitsPerCore).toInt, regionCount)
  }

  /**
   * Get the size of a HBase table on disk
   *
   * @param hBaseConfig HBase configuration
   * @param tableName HBase table name
   * @return Size of HBase table on disk
   */
  @VisibleForTesting
  def getTableSizeInMb(hBaseConfig: org.apache.hadoop.conf.Configuration, tableName: TableName): Long = {
    val tableSize = Try({
      val tableDir = FSUtils.getTableDir(FSUtils.getRootDir(hBaseConfig), tableName)
      println("Table dir:" + tableDir)
      val fileSystem = FileSystem.get(hBaseConfig)
      fileSystem.getContentSummary(tableDir).getLength / (1024 * 1024)
    }).getOrElse(0L)

    println("Table size:" + tableSize)
    tableSize
  }

  /**
   * Get the maximum number of cores available to the Spark cluster
   *
   * @param sparkContext Spark context
   * @param hBaseAdmin HBase administration
   * @return Available Spark cores
   */
  private def getMaxSparkCores(sparkContext: SparkContext, hBaseAdmin: HBaseAdmin): Int = {
    val configuredMaxCores = sparkContext.getConf.getInt(SPARK_MAX_CORES, 0)

    val maxSparkCores = if (configuredMaxCores > 0) {
      configuredMaxCores
    }
    else {
      // val numWorkers = sparkContext.getExecutorStorageStatus.size -1
      // getExecutorStorageStatus not working correctly and sometimes returns 1 instead of the correct number of workers
      // Using number of region servers to estimate the number of slaves since it is more reliable
      val numWorkers = getHBaseRegionServerCount(hBaseAdmin)
      val numCoresPerWorker = Runtime.getRuntime.availableProcessors()
      numCoresPerWorker * numWorkers
    }
    Math.max(0, maxSparkCores)
  }

  /**
   * Get HBase pre-splits
   *
   * Uses a simple rule-of-thumb to calculate the number of regions per table. This is a place-holder
   * while we figure out how to incorporate other parameters such as input-size, and number of HBase column
   * families into the formula.
   *
   * @param hBaseAdmin HBase administration
   * @return Number of HBase pre-splits
   */
  private def getHBasePreSplits(hBaseAdmin: HBaseAdmin): Int = {
    val regionsPerServer = titanConfig.getInt(HBASE_REGIONS_PER_SERVER, 1)
    val regionServerCount = getHBaseRegionServerCount(hBaseAdmin)

    val preSplits = if (regionServerCount > 0) regionsPerServer * regionServerCount else 0

    preSplits
  }

  /**
   * Get the number of region servers in the HBase cluster.
   *
   * @param hBaseAdmin HBase administration
   * @return Number of region servers
   */
  private def getHBaseRegionServerCount(hBaseAdmin: HBaseAdmin): Int = Try({
    hBaseAdmin.getClusterStatus() getServersSize
  }).getOrElse(-1)

}

/**
 * Constants used by Titan Auto-partitioner
 */
object TitanAutoPartitioner {
  val ENABLE_AUTO_PARTITION = "auto-partitioner.enable"
  val HBASE_REGIONS_PER_SERVER = "auto-partitioner.hbase.regions-per-server"
  val HBASE_INPUT_SPLITS_PER_CORE = "auto-partitioner.hbase.input-splits-per-spark-core"
  val SPARK_MAX_CORES = "spark.cores.max"
  val TITAN_HBASE_REGION_COUNT = "storage.hbase.region-count"
}
