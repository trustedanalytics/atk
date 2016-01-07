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
import org.trustedanalytics.atk.engine.partitioners.{ FileSizeToPartitionSize, SparkAutoPartitionStrategy }
import com.typesafe.config.{ ConfigFactory }
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils

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

  // val's are not lazy because failing early is better

  /** URL for spark master, e.g. "spark://hostname:7077", "local[4]", etc */
  val sparkMaster: String = {
    val sparkMaster = config.getString("trustedanalytics.atk.engine.spark.master")
    if (StringUtils.isEmpty(sparkMaster)) {
      "spark://" + hostname + ":7077"
    }
    else {
      sparkMaster
    }
  }

  val isSparkOnYarn: Boolean = {
    "yarn-cluster".equalsIgnoreCase(sparkMaster) || "yarn-client".equalsIgnoreCase(sparkMaster)
  }

  /** Spark home directory, e.g. "/opt/cloudera/parcels/CDH/lib/spark", "/usr/lib/spark", etc. */
  val sparkHome: String = config.getString("trustedanalytics.atk.engine.spark.home")
  val hiveLib: String = config.getString("trustedanalytics.atk.engine.hive.lib")
  val hiveConf: String = config.getString("trustedanalytics.atk.engine.hive.conf")
  val jdbcLib: String = config.getString("trustedanalytics.atk.engine.jdbc.lib")
  val sparkBroadcastFactoryLib: String = config.getString("trustedanalytics.atk.engine.spark.broadcast-factory-lib")

  /** Path to hbase conf, e.g. /etc/hbase/conf */
  val hbaseConf: String = config.getString("trustedanalytics.atk.engine.hbase.configuration.path")

  /**
   * New in-progress feature for keeping Yarn Application Master alive between requests.
   *
   * This is a feature flag so we can begin adding code to the master branch without breaking
   * current application or needing long-lived feature branches
   */
  val keepYarnJobAlive: Boolean = config.getBoolean("trustedanalytics.atk.engine.keep-yarn-job-alive")

  /**
   * After this many minutes Yarn job will be shutdown if there was no further activity
   */
  val yarnWaitTimeout: Long = config.getLong("trustedanalytics.atk.engine.yarn-wait-timeout")

  /**
   * true to re-use a SparkContext, this can be helpful for automated integration tests, not for customers.
   * NOTE: true should break the progress bar.
   */
  val reuseSparkContext: Boolean = config.getBoolean("trustedanalytics.atk.engine.spark.reuse-context")

  /**
   * The hdfs URL where the 'trustedanalytics' folder will be created and which will be used as the starting
   * point for any relative URLs e.g. "hdfs://hostname/user/atkuser"
   */
  val fsRoot: String = config.getString("trustedanalytics.atk.engine.fs.root")

  /**
   * Path relative to fs.root where jars are copied to
   */
  val hdfsLib: String = config.getString("trustedanalytics.atk.engine.hdfs-lib")

  val pageSize: Int = config.getInt("trustedanalytics.atk.engine.page-size")

  /**
   * Number of rows taken for sample test during frame loading
   */
  val frameLoadTestSampleSize: Int =
    config.getInt("trustedanalytics.atk.engine.command.frames.load.config.schema-validation-sample-rows")

  /**
   * Percentage of maximum rows fail in parsing in sampling test. 50 means up 50% is allowed
   */
  val frameLoadTestFailThresholdPercentage: Int =
    config.getInt("trustedanalytics.atk.engine.command.frames.load.config.schema-validation-fail-threshold-percentage")

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
  val maxPartitions: Int = config.getInt("trustedanalytics.atk.engine.auto-partitioner.max-partitions")

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
  val pythonWorkerExec: String = config.getString("trustedanalytics.atk.engine.spark.python-worker-exec")
  val pythonUdfDependenciesDirectory: String = config.getString("trustedanalytics.atk.engine.spark.python-udf-deps-directory")
  val pythonDefaultDependencySearchDirectories: List[String] =
    config.getList("trustedanalytics.atk.engine.spark.python-default-deps-search-directories").map(_.render).toList

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
      case StringUtils.EMPTY => throw new IllegalArgumentException(key + " cannot be empty!")
      case s: String => s
    }
  }

  //gc variables
  lazy val gcInterval = config.getDuration("trustedanalytics.atk.engine.gc.interval", TimeUnit.MILLISECONDS)
  lazy val gcStaleAge = config.getDuration("trustedanalytics.atk.engine.gc.stale-age", TimeUnit.MILLISECONDS)

  val enableKerberos: Boolean = config.getBoolean("trustedanalytics.atk.engine.hadoop.kerberos.enabled")
  val kerberosPrincipalName: Option[String] = if (enableKerberos) Some(nonEmptyString("trustedanalytics.atk.engine.hadoop.kerberos.principal-name")) else None
  val kerberosKeyTabPath: Option[String] = if (enableKerberos) Some(nonEmptyString("trustedanalytics.atk.engine.hadoop.kerberos.keytab-file")) else None

  /**
   * Path to effective application.conf (includes overrides passed in at runtime
   * that will need to be sent to Yarn)
   */
  lazy val effectiveApplicationConf: String = {
    val file = File.createTempFile("effective-application-conf-", ".tmp.conf")
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

  /** The file name without path */
  lazy val effectiveApplicationConfFileName: String = {
    StringUtils.substringAfterLast(effectiveApplicationConf, File.separator)
  }

}
