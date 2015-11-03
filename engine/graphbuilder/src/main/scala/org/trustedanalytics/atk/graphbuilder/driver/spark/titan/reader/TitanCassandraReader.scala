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
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.TitanReaderRdd
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader.TitanReader._
import org.trustedanalytics.atk.graphbuilder.elements.GraphElement
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import com.thinkaurelius.titan.diskstorage.Backend
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.thinkaurelius.titan.hadoop.formats.cassandra.TitanCassandraInputFormat
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{ SlicePredicate, SliceRange }
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * This is a TitanReader that runs on Spark, and reads a Titan graph from a Cassandra storage backend.
 *
 * @param sparkContext Spark context
 * @param titanConnector Connector to Titan
 */
class TitanCassandraReader(sparkContext: SparkContext, titanConnector: TitanGraphConnector) extends TitanReader(sparkContext, titanConnector) {
  require(titanConfig.containsKey(TITAN_STORAGE_HOSTNAME), "could not find key " + TITAN_STORAGE_HOSTNAME)
  require(titanConfig.containsKey(TITAN_STORAGE_CASSANDRA_KEYSPACE), "could not find key " + TITAN_STORAGE_CASSANDRA_KEYSPACE)

  /**
   * Read Titan graph from a Cassandra storage backend into a Spark RDD of graph elements.
   *
   * The RDD returns an iterable of both vertices and edges using GraphBuilder's GraphElement trait. The GraphElement
   * trait is an interface implemented by both vertices and edges.
   *
   * @return RDD of GraphBuilder elements
   */
  override def read(): RDD[GraphElement] = {
    val cassandraConfig = createCassandraConfiguration()

    val cassandraRDD = sparkContext.newAPIHadoopRDD(cassandraConfig, classOf[TitanCassandraInputFormat],
      classOf[NullWritable],
      classOf[FaunusVertex])

    new TitanReaderRdd(cassandraRDD, titanConnector)
  }

  /**
   * Create Cassandra configuration for connecting to Cassandra table
   */
  private def createCassandraConfiguration(): org.apache.hadoop.conf.Configuration = {
    val cassandraConfig = sparkContext.hadoopConfiguration //new org.apache.hadoop.conf.Configuration()
    val tableName = titanConfig.getString(TITAN_STORAGE_CASSANDRA_KEYSPACE)
    val port = titanConfig.getString(TITAN_STORAGE_PORT)
    val hostnames = titanConfig.getString(TITAN_STORAGE_HOSTNAME)
    val wideRows = titanConfig.getBoolean(TITAN_CASSANDRA_INPUT_WIDEROWS, false)
    val predicate: SlicePredicate = getSlicePredicate

    ConfigHelper.setInputSlicePredicate(cassandraConfig, predicate)
    ConfigHelper.setInputPartitioner(cassandraConfig, "Murmur3Partitioner")
    ConfigHelper.setOutputPartitioner(cassandraConfig, "Murmur3Partitioner")
    ConfigHelper.setInputRpcPort(cassandraConfig, port)
    ConfigHelper.setInputColumnFamily(cassandraConfig, tableName, Backend.EDGESTORE_NAME, wideRows)

    titanConfig.getKeys.foreach {
      case (titanKey: String) =>
        val titanHadoopKey = TITAN_HADOOP_PREFIX + titanKey
        cassandraConfig.set(titanHadoopKey, titanConfig.getProperty(titanKey).toString)
    }

    cassandraConfig
  }

  /**
   * Get Range of rows (Slice predicate) for Cassandra input format
   * @return Slice predicate
   */
  private def getSlicePredicate: SlicePredicate = {
    val predicate = new SlicePredicate()
    val sliceRange = new SliceRange()
    sliceRange.setStart(Array.empty[Byte])
    sliceRange.setFinish(Array.empty[Byte])
    predicate.setSlice_range(sliceRange)
  }
}
