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


package org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader

import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.titan.io.GBTitanHBaseInputFormat
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.TitanReaderRdd
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader.TitanReader._
import org.trustedanalytics.atk.graphbuilder.elements.GraphElement
import org.trustedanalytics.atk.graphbuilder.graph.titan.{ TitanHadoopHBaseCacheListener, TitanAutoPartitioner, TitanGraphConnector }
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ SparkListenerApplicationEnd, SparkListener }

import scala.collection.JavaConversions._

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a HBase storage backend.
 *
 * @param sparkContext Spark context
 * @param titanConnector Connector to Titan
 */
class TitanHBaseReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends TitanReader(sparkContext, titanConnector) {
  require(titanConfig.containsKey(TITAN_STORAGE_HOSTNAME), "could not find key " + TITAN_STORAGE_HOSTNAME)
  require(titanConfig.containsKey(TITAN_STORAGE_HBASE_TABLE), "could not find key " + TITAN_STORAGE_HBASE_TABLE)

  /**
   * Read Titan graph from a HBase storage backend into a Spark RDD of graph elements.
   *
   * The RDD returns an iterable of both vertices and edges using GraphBuilder's GraphElement trait. The GraphElement
   * trait is an interface implemented by both vertices and edges.
   *
   * @return RDD of GraphBuilder elements
   */
  override def read(): RDD[GraphElement] = {
    val hBaseConfig = createHBaseConfiguration()
    val tableName = titanConfig.getString(TITAN_STORAGE_HBASE_TABLE)

    checkTableExists(hBaseConfig, tableName)

    val hBaseRDD = sparkContext.newAPIHadoopRDD(hBaseConfig, classOf[GBTitanHBaseInputFormat],
      classOf[NullWritable],
      classOf[FaunusVertex])

    sparkContext.addSparkListener(new TitanHadoopHBaseCacheListener())
    new TitanReaderRdd(hBaseRDD, titanConnector)

  }

  /**
   * Create HBase configuration for connecting to HBase table
   */
  private def createHBaseConfiguration(): org.apache.hadoop.conf.Configuration = {
    val hBaseConfig = HBaseConfiguration.create()

    // Add Titan configuratoin
    titanConfig.getKeys.foreach {
      case (titanKey: String) =>
        val titanHadoopKey = TITAN_HADOOP_PREFIX + titanKey
        hBaseConfig.set(titanHadoopKey, titanConfig.getProperty(titanKey).toString)
    }

    // Auto-configure number of input splits
    val tableName = titanConfig.getString(TITAN_STORAGE_HBASE_TABLE)
    val titanAutoPartitioner = TitanAutoPartitioner(titanConfig)
    titanAutoPartitioner.setSparkHBaseInputSplits(sparkContext, hBaseConfig, tableName)

    hBaseConfig
  }

  /**
   * Throw an exception if the HBase table does not exist.
   *
   * @param hBaseConfig HBase configuration
   * @param tableName HBase table name
   */
  private def checkTableExists(hBaseConfig: org.apache.hadoop.conf.Configuration, tableName: String) = {
    val admin = new HBaseAdmin(hBaseConfig)
    if (!admin.isTableAvailable(tableName)) {
      admin.close()
      throw new RuntimeException("HBase table does not exist: " + tableName + " (graph may not have been loaded with any data)")
    }
    admin.close()
  }
}
