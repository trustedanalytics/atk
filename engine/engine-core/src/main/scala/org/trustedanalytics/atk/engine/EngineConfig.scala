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

package org.trustedanalytics.atk.engine

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import org.trustedanalytics.atk.event.{ EventContext, EventLogging }
import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanAutoPartitioner
import org.trustedanalytics.atk.engine.partitioners.{ FileSizeToPartitionSize, SparkAutoPartitionStrategy }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import scala.collection.JavaConverters._
import scala.concurrent.duration._

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

  val config = ConfigFactory.load()

  // val's are not lazy because failing early is better

  /** URL for spark master, e.g. "spark://hostname:7077", "local[4]", etc */
  val sparkMaster: String = {
    val sparkMaster = config.getString("trustedanalytics.atk.engine.spark.master")
    if (sparkMaster == "") {
      "spark://" + hostname + ":7077"
    }
    else {
      sparkMaster
    }
  }

  val isLocalMaster: Boolean = {
    (sparkMaster.startsWith("local[") && sparkMaster.endsWith("]")) || sparkMaster.equals("local")
  }

  val isSparkOnYarn: Boolean = sparkMaster == "yarn-cluster" || sparkMaster == "yarn-client"

  /** Spark home directory, e.g. "/opt/cloudera/parcels/CDH/lib/spark", "/usr/lib/spark", etc. */
  val sparkHome: String = config.getString("trustedanalytics.atk.engine.spark.home")

  val hiveLib: String = {
    config.getString("trustedanalytics.atk.engine.hive.lib")
  }

  val hiveConf: String = {
    config.getString("trustedanalytics.atk.engine.hive.conf")
  }

  /**
   * true to re-use a SparkContext, this can be helpful for automated integration tests, not for customers.
   * NOTE: true should break the progress bar.
   */
  val reuseSparkContext: Boolean = config.getBoolean("trustedanalytics.atk.engine.spark.reuse-context")

  val defaultTimeout: FiniteDuration = config.getInt("trustedanalytics.atk.engine.default-timeout").seconds

  /**
   * The hdfs URL where the 'trustedanalytics' folder will be created and which will be used as the starting
   * point for any relative URLs e.g. "hdfs://hostname/user/atkuser"
   */
  val fsRoot: String = config.getString("trustedanalytics.atk.engine.fs.root")

  /**
   * Absolute local paths where jars are copied from
   */
  val localLibs: List[String] = config.getStringList("trustedanalytics.atk.engine.local-libs").asScala.toList

  /**
   * Path relative to fs.root where jars are copied to
   */
  val hdfsLib: String = config.getString("trustedanalytics.atk.engine.hdfs-lib")

  val pageSize: Int = config.getInt("trustedanalytics.atk.engine.page-size")

  /* number of rows taken for sample test during frame loading */
  val frameLoadTestSampleSize: Int =
    config.getInt("trustedanalytics.atk.engine.command.frames.load.config.schema-validation-sample-rows")

  /* percentage of maximum rows fail in parsing in sampling test. 50 means up 50% is allowed */
  val frameLoadTestFailThresholdPercentage: Int =
    config.getInt("trustedanalytics.atk.engine.command.frames.load.config.schema-validation-fail-threshold-percentage")

  /**
   * A list of archives that will be searched for command plugins
   */
  val archives: List[String] = {
    config.getStringList("trustedanalytics.atk.engine.plugin.command.archives")
      .asScala
      .toList
  }

  /**
   * Default settings for Titan Load.
   *
   * Creates a new configuration bean each time so it can be modified by the caller (like setting the table name).
   */
  def titanLoadConfiguration: SerializableBaseConfiguration = {
    createTitanConfiguration(config, "trustedanalytics.atk.engine.titan.load")
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
    val properties = config.getConfig("trustedanalytics.atk.engine.spark.conf.properties")
    for (entry <- properties.entrySet().asScala) {
      sparkConfProperties += entry.getKey -> properties.getString(entry.getKey)
    }
    sparkConfProperties
  }

  /**
   * Max partitions if file is larger than limit specified in autoPartitionConfig
   */
  val maxPartitions: Int = {
    config.getInt("trustedanalytics.atk.engine.auto-partitioner.max-partitions")
  }

  /**
   * Disable all kryo registration in plugins (this is mainly here for performance testing
   * and debugging when someone suspects Kryo might be causing some kind of issue).
   */
  val disableKryo: Boolean = config.getBoolean("trustedanalytics.atk.engine.spark.disable-kryo")

  /**
   * Sorted list of mappings for file size to partition size (larger file sizes first)
   */
  val autoPartitionerConfig: List[FileSizeToPartitionSize] = {
    import scala.collection.JavaConverters._
    val key = "trustedanalytics.atk.engine.auto-partitioner.file-size-to-partition-size"
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
    val strategyName = config.getString("trustedanalytics.atk.engine.auto-partitioner.repartition.strategy")
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
    val repartitionPercentage = config.getInt("trustedanalytics.atk.engine.auto-partitioner.repartition.threshold-percent")
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
    val ratio = config.getDouble("trustedanalytics.atk.engine.auto-partitioner.repartition.frame-compression-ratio")
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
    val tasksPerCore = config.getInt("trustedanalytics.atk.engine.auto-partitioner.default-tasks-per-core")
    if (tasksPerCore > 0) tasksPerCore else throw new RuntimeException(s"Default tasks per core should be greater than 0")
  }

  /**
   * Use broadcast join if file size is lower than threshold.
   *
   * A threshold of zero disables broadcast joins. This threshold should not exceed the maximum size of results
   * that can be returned to the Spark driver (i.e., spark.driver.maxResultSize).
   */
  val broadcastJoinThreshold = {
    val joinThreshold = config.getBytes("trustedanalytics.atk.engine.auto-partitioner.broadcast-join-threshold")
    val maxResultSize = config.getBytes("trustedanalytics.atk.engine.spark.conf.properties.spark.driver.maxResultSize")
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
  val sparkDriverMaxPermSize = config.getString("trustedanalytics.atk.engine.spark.conf.properties.spark.driver.maxPermSize")

  val sparkOnYarnNumExecutors = config.getInt("trustedanalytics.atk.engine.spark.conf.properties.spark.yarn.numExecutors")

  /**
   * Determines whether SparkContex.addJars() paths get "local:" prefix or not.
   *
   * True if engine-core.jar, interfaces.jar and ohters are installed locally on each cluster node (preferred).
   * False is useful mainly for development on a cluster.  False results in many copies of the application jars
   * being made and copied to all of the cluster nodes.
   */
  val sparkAppJarsLocal: Boolean = config.getBoolean("trustedanalytics.atk.engine.spark.app-jars-local")

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
    if (sparkAppJarsLocal) {
      info("sparkAppJarsLocal: " + sparkAppJarsLocal + " (expecting application jars to be installed on all worker nodes)")
    }
    else {
      info("sparkAppJarsLocal: " + sparkAppJarsLocal + " (application jars will be copied to worker nodes with every command)")
    }
  }

  // Python execution command for workers
  val pythonWorkerExec: String = config.getString("trustedanalytics.atk.engine.spark.python-worker-exec")
  val pythonUdfDependenciesDirectory: String = config.getString("trustedanalytics.atk.engine.spark.python-udf-deps-directory")

  // val's are not lazy because failing early is better
  val metaStoreConnectionUrl: String = nonEmptyString("trustedanalytics.atk.metastore.connection.url")
  val metaStoreConnectionDriver: String = nonEmptyString("trustedanalytics.atk.metastore.connection.driver")
  val metaStoreConnectionUsername: String = config.getString("trustedanalytics.atk.metastore.connection.username")
  val metaStoreConnectionPassword: String = config.getString("trustedanalytics.atk.metastore.connection.password")
  val metaStorePoolMaxActive: Int = config.getInt("trustedanalytics.atk.metastore.pool.max-active")

  /**
   * Get a String but throw Exception if it is empty
   */
  protected def nonEmptyString(key: String): String = {
    config.getString(key) match {
      case "" => throw new IllegalArgumentException(key + " cannot be empty!")
      case s: String => s
    }
  }

  //gc variables
  val gcInterval = config.getDuration("trustedanalytics.atk.engine.gc.interval", TimeUnit.MILLISECONDS)
  val gcAgeToDeleteData = config.getDuration("trustedanalytics.atk.engine.gc.data-lifespan", TimeUnit.MILLISECONDS)

  val enableKerberos: Boolean = config.getBoolean("trustedanalytics.atk.engine.hadoop.kerberos.enabled")
  val kerberosPrincipalName: Option[String] = if (enableKerberos) Some(nonEmptyString("trustedanalytics.atk.engine.hadoop.kerberos.principal-name")) else None
  val kerberosKeyTabPath: Option[String] = if (enableKerberos) Some(nonEmptyString("trustedanalytics.atk.engine.hadoop.kerberos.keytab-file")) else None

}
