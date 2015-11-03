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

import java.io.File
import java.net.InetAddress
import java.util.concurrent.TimeUnit

import org.apache.commons.io.FileUtils
import org.trustedanalytics.atk.event.{ EventContext, EventLogging }
import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanAutoPartitioner
import org.trustedanalytics.atk.engine.partitioners.{ FileSizeToPartitionSize, SparkAutoPartitionStrategy }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import org.apache.commons.lang.StringUtils

/**
 * Configuration Settings for the SparkEngine,
 *
 * This is our wrapper for Typesafe config.
 */
object EngineConfig extends EngineConfig

/**
 * Configuration Settings for the SparkEngine,
 *
 * This is our wrapper for Typesafe config.
 */
trait EngineConfig extends EventLogging {
  implicit val eventContext: EventContext = null

  val config = ConfigFactory.load(this.getClass.getClassLoader)
  val engineConfigKey = "trustedanalytics.atk.engine"
  val sparkConfigKey = engineConfigKey + ".spark"
  val hiveConfigKey = engineConfigKey + ".hive"
  val jdbcConfigKey = engineConfigKey + ".jdbc"
  val autoPartitionKey = engineConfigKey + ".auto-partitioner"
  val metaStoreRoot = "trustedanalytics.atk.metastore"
  val metaStoreConnection = metaStoreRoot + ".connection"

  // val's are not lazy because failing early is better

  /** URL for spark master, e.g. "spark://hostname:7077", "local[4]", etc */
  val sparkMaster: String = {
    val sparkMaster = config.getString(sparkConfigKey + ".master")
    if (StringUtils.isEmpty(sparkMaster)) {
      "spark://" + hostname + ":7077"
    }
    else {
      sparkMaster
    }
  }

  val isLocalMaster: Boolean = {
    (sparkMaster.startsWith("local[") && sparkMaster.endsWith("]")) || sparkMaster.equals("local")
  }

  val isSparkOnYarn: Boolean = {
    "yarn-cluster".equalsIgnoreCase(sparkMaster) || "yarn-client".equalsIgnoreCase(sparkMaster)
  }

  /** Spark home directory, e.g. "/opt/cloudera/parcels/CDH/lib/spark", "/usr/lib/spark", etc. */
  val sparkHome: String = config.getString(sparkConfigKey + ".home")
  val hiveLib: String = config.getString(hiveConfigKey + ".lib")
  val hiveConf: String = config.getString(hiveConfigKey + ".conf")
  val jdbcLib: String = config.getString(jdbcConfigKey + ".lib")

  /**
   * true to re-use a SparkContext, this can be helpful for automated integration tests, not for customers.
   * NOTE: true should break the progress bar.
   */
  val reuseSparkContext: Boolean = config.getBoolean(sparkConfigKey + ".reuse-context")
  val defaultTimeout: FiniteDuration = config.getInt(engineConfigKey + ".default-timeout").seconds

  /**
   * The hdfs URL where the 'trustedanalytics' folder will be created and which will be used as the starting
   * point for any relative URLs e.g. "hdfs://hostname/user/atkuser"
   */
  val fsRoot: String = config.getString(engineConfigKey + ".fs.root")

  /**
   * Path relative to fs.root where jars are copied to
   */
  val hdfsLib: String = config.getString(engineConfigKey + ".hdfs-lib")

  val pageSize: Int = config.getInt(engineConfigKey + ".page-size")

  /**
   * Number of rows taken for sample test during frame loading
   */
  val frameLoadTestSampleSize: Int =
    config.getInt(engineConfigKey + ".command.frames.load.config.schema-validation-sample-rows")

  /**
   * Percentage of maximum rows fail in parsing in sampling test. 50 means up 50% is allowed
   */
  val frameLoadTestFailThresholdPercentage: Int =
    config.getInt(engineConfigKey + ".command.frames.load.config.schema-validation-fail-threshold-percentage")

  /**
   * Default settings for Titan Load.
   *
   * Creates a new configuration bean each time so it can be modified by the caller (like setting the table name).
   */
  def titanLoadConfiguration: SerializableBaseConfiguration = {
    createTitanConfiguration(config, engineConfigKey + ".titan.load")
  }

  /**
   * Create new configuration for Titan using properties specified in path expression.
   *
   * This method can also be used by command plugins in the Spark engine which might use
   * a different configuration object.
   *
   * @param commandConfig Configuration object for command.
   * @param titanPath Dot-separated expressions with Titan config, e.g., trustedanalytics.atk.engine.titan.load
   * @return Titan configuration
   */
  def createTitanConfiguration(commandConfig: Config, titanPath: String): SerializableBaseConfiguration = {
    val titanConfiguration = new SerializableBaseConfiguration
    val titanDefaultConfig = commandConfig.getConfig(titanPath)

    //Prevents errors in Titan/HBase reader when storage.hostname is converted to list
    titanConfiguration.setDelimiterParsingDisabled(true)
    for (entry <- titanDefaultConfig.entrySet().asScala) {
      titanConfiguration.addProperty(entry.getKey, titanDefaultConfig.getString(entry.getKey))
    }

    setTitanAutoPartitions(titanConfiguration)
  }

  /**
   * Update Titan configuration with auto-generated settings.
   *
   * At present, auto-partitioner for graph construction only sets HBase pre-splits.
   *
   * @param titanConfiguration the config to be modified
   * @return Updated Titan configuration
   */
  def setTitanAutoPartitions(titanConfiguration: SerializableBaseConfiguration): SerializableBaseConfiguration = {
    val titanAutoPartitioner = TitanAutoPartitioner(titanConfiguration)
    val storageBackend = titanConfiguration.getString("storage.backend")

    storageBackend.toLowerCase match {
      case "hbase" =>
        val hBaseAdmin = new HBaseAdmin(HBaseConfiguration.create())
        titanAutoPartitioner.setHBasePreSplits(hBaseAdmin)
        info("Setting Titan/HBase pre-splits for  to: " + titanConfiguration.getProperty(TitanAutoPartitioner.TITAN_HBASE_REGION_COUNT))
      case _ => info("No auto-configuration settings for storage backend: " + storageBackend)
    }

    titanConfiguration.setProperty("auto-partitioner.broadcast-join-threshold", broadcastJoinThreshold)

    titanConfiguration
  }

  /**
   * Configuration properties that will be supplied to SparkConf()
   */
  val sparkConfProperties: Map[String, String] = {
    var sparkConfProperties = Map[String, String]()
    val properties = config.getConfig(sparkConfigKey + ".conf.properties")
    for (entry <- properties.entrySet().asScala) {
      sparkConfProperties += entry.getKey -> properties.getString(entry.getKey)
    }
    sparkConfProperties
  }

  /**
   * Max partitions if file is larger than limit specified in autoPartitionConfig
   */
  val maxPartitions: Int = config.getInt(autoPartitionKey + ".max-partitions")

  /**
   * Disable all kryo registration in plugins (this is mainly here for performance testing
   * and debugging when someone suspects Kryo might be causing some kind of issue).
   */
  val disableKryo: Boolean = config.getBoolean(sparkConfigKey + ".disable-kryo")

  /**
   * Sorted list of mappings for file size to partition size (larger file sizes first)
   */
  val autoPartitionerConfig: List[FileSizeToPartitionSize] = {
    import scala.collection.JavaConverters._
    val key = autoPartitionKey + ".file-size-to-partition-size"
    val configs = config.getConfigList(key).asScala.toList
    val unsorted = configs.map(config => {
      val partitions = config.getInt("partitions")
      if (partitions > maxPartitions) {
        throw new RuntimeException("Invalid value partitions:" + partitions +
          " shouldn't be larger than max-partitions:" + maxPartitions + ", under:" + key)
      }
      FileSizeToPartitionSize(config.getBytes("upper-bound"), partitions)
    })
    unsorted.sortWith((leftConfig, rightConfig) => leftConfig.fileSizeUpperBound > rightConfig.fileSizeUpperBound)
  }

  /**
   * Minimum number of partitions that can be set by the Spark auto-partitioner
   */
  val minPartitions: Int = 2

  /**
   * Spark re-partitioning strategy
   */
  val repartitionStrategy: SparkAutoPartitionStrategy.PartitionStrategy = {
    val strategyName = config.getString(autoPartitionKey + ".repartition.strategy")
    val strategy = SparkAutoPartitionStrategy.getRepartitionStrategy(strategyName)

    info("Setting Spark re-partitioning strategy to: " + strategy)
    strategy
  }

  /**
   * Spark re-partitioning threshold
   *
   * Re-partition RDD if the percentage difference between actual and desired partitions exceeds threshold
   */
  val repartitionThreshold: Double = {
    val repartitionPercentage = config.getInt(autoPartitionKey + ".repartition.threshold-percent")
    if (repartitionPercentage >= 0 && repartitionPercentage <= 100) {
      repartitionPercentage / 100d
    }
    else {
      throw new RuntimeException(s"Repartition threshold-percent should not be less than 0, or greater than 100")
    }
  }

  /**
   * Frame compression ratio
   *
   * Used to estimate actual size of the frame for compressed file formats like Parquet.
   * This ratio prevents us from under-estimating the number of partitions for compressed files.
   * compression-ratio=uncompressed-size/compressed-size
   * e.g., compression-ratio=4 if  uncompressed size is 20MB, and compressed size is 5MB
   */
  val frameCompressionRatio: Double = {
    val ratio = config.getDouble(autoPartitionKey + ".repartition.frame-compression-ratio")
    if (ratio <= 0) throw new RuntimeException("frame-compression-ratio should be greater than zero")
    ratio
  }

  /**
   * Default Spark tasks-per-core
   *
   * Used by some plugins to set the number of partitions to default-tasks-per-core*Spark cores
   * The performance of some plugins (e.g. group by) degrades significantly if there are too many partitions
   * relative to available cores (especially in the reduce phase)
   */
  val maxTasksPerCore: Int = {
    val tasksPerCore = config.getInt(autoPartitionKey + ".default-tasks-per-core")
    if (tasksPerCore > 0) tasksPerCore else throw new RuntimeException(s"Default tasks per core should be greater than 0")
  }

  /**
   * Use broadcast join if file size is lower than threshold.
   *
   * A threshold of zero disables broadcast joins. This threshold should not exceed the maximum size of results
   * that can be returned to the Spark driver (i.e., spark.driver.maxResultSize).
   */
  val broadcastJoinThreshold = {
    val joinThreshold = config.getBytes(autoPartitionKey + ".broadcast-join-threshold")
    val maxResultSize = config.getBytes(sparkConfigKey + ".conf.properties.spark.driver.maxResultSize")
    if (joinThreshold > maxResultSize) {
      throw new RuntimeException(
        s"Broadcast join threshold: $joinThreshold shouldn't be larger than spark.driver.maxResultSize: $maxResultSize")
    }
    joinThreshold
  }

  /**
   * spark driver max size should be minimum of 128M for Spark Submit to work. We are currently loading multiple
   * class loaders and Spark Submit driver throws OOM if default value of 64M is kept for PermGen space
   */
  val sparkDriverMaxPermSize = config.getString(sparkConfigKey + ".conf.properties.spark.driver.maxPermSize")

  /** Fully qualified Hostname for current system */
  private def hostname: String = InetAddress.getLocalHost.getCanonicalHostName

  // log important settings
  def logSettings(): Unit = withContext("EngineConfig") {
    info("fsRoot: " + fsRoot)
    info("sparkHome: " + sparkHome)
    info("sparkMaster: " + sparkMaster)
    info("disableKryo: " + disableKryo)
    for ((key: String, value: String) <- sparkConfProperties) {
      info(s"sparkConfProperties: $key = $value")
    }
  }

  // Python execution command for workers
  val pythonWorkerExec: String = config.getString(sparkConfigKey + ".python-worker-exec")
  val pythonUdfDependenciesDirectory: String = config.getString(sparkConfigKey + ".python-udf-deps-directory")
  val pythonDefaultDependencySearchDirectories: List[String] =
    config.getList(sparkConfigKey + ".python-default-deps-search-directories").map(_.render).toList

  // val's are not lazy because failing early is better
  val metaStoreConnectionUrl: String = nonEmptyString(metaStoreConnection + ".url")
  val metaStoreConnectionDriver: String = nonEmptyString(metaStoreConnection + ".driver")
  val metaStoreConnectionUsername: String = config.getString(metaStoreConnection + ".username")
  val metaStoreConnectionPassword: String = config.getString(metaStoreConnection + ".password")
  val metaStorePoolMaxActive: Int = config.getInt(metaStoreRoot + ".pool.max-active")

  /**
   * Get a String but throw Exception if it is empty
   */
  protected def nonEmptyString(key: String): String = {
    config.getString(key) match {
      case StringUtils.EMPTY => throw new IllegalArgumentException(key + " cannot be empty!")
      case s: String => s
    }
  }

  //gc variables
  lazy val gcInterval = config.getDuration(engineConfigKey + ".gc.interval", TimeUnit.MILLISECONDS)
  lazy val gcStaleAge = config.getDuration(engineConfigKey + ".gc.stale-age", TimeUnit.MILLISECONDS)

  val enableKerberos: Boolean = config.getBoolean(engineConfigKey + ".hadoop.kerberos.enabled")
  val kerberosPrincipalName: Option[String] = if (enableKerberos) Some(nonEmptyString(engineConfigKey + ".hadoop.kerberos.principal-name")) else None
  val kerberosKeyTabPath: Option[String] = if (enableKerberos) Some(nonEmptyString(engineConfigKey + ".hadoop.kerberos.keytab-file")) else None

  /**
   * Path to effective application.conf (includes overrides passed in at runtime
   * that will need to be sent to Yarn)
   */
  lazy val effectiveApplicationConf: String = {
    val file = File.createTempFile("effective-application-conf-", ".tmp")
    file.deleteOnExit()

    // modify permissions since this file could have credentials in it
    // first remove all permissions
    file.setReadable(false, false)
    file.setWritable(false, false)
    file.setExecutable(false, false)

    // then add owner permissions
    file.setReadable(true, true)
    file.setWritable(true, true)

    FileUtils.writeStringToFile(file, config.root().render())

    file.getAbsolutePath
  }
}
