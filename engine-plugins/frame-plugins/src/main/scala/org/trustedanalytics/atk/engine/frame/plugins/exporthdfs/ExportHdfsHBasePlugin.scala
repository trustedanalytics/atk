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

package org.trustedanalytics.atk.engine.frame.plugins.exporthdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.{RDD, PairRDDFunctions}
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.{ ExportHdfsHBaseArgs }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkContext._

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Export a frame to hive table
 */
@PluginDoc(oneLine = "Write current frame to HBase table.",
  extended = """Table must not exist in HBase.
Export of Vectors is not currently supported.""")
class ExportHdfsHBasePlugin extends SparkCommandPlugin[ExportHdfsHBaseArgs, UnitReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_to_hbase"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportHdfsHBaseArgs)(implicit invocation: Invocation) = 5

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsHBaseArgs)(implicit invocation: Invocation): UnitReturn = {

    val frame: SparkFrame = arguments.frame
    val rdd = frame.rdd
    val columns = rdd.frameSchema.columnNames
    val columnTypes = rdd.frameSchema.columns.map (_.dataType)

    val conf = createConfig(arguments.tableName)
    val rawValues = rdd.mapRows(row => row.values())
    // 1. Conver this rawValues List[Any] => List[Datatype]
    // 2. Convert List[Datatype] Bytes.toBytes(value)
    // 3. RDD[List[Array[Byte]]
    // 4. Ask user which column is row key

    val pairRdd = rawValues.map((_, ("")))



//    val pairRDD = rdd.map (row => {
//      val values = row.
//      val put = new Put(null)
//    })

    pairRdd.saveAsNewAPIHadoopDataset(conf)

    //new PairRDDFunctions(rdd.map(null)).saveAsNewAPIHadoopDataset (conf)
  }

  /**
   * Create initial configuration for bHase reader
   * @param tableName name of hBase table
   * @return hBase configuration
   */
  private def createConfig(tableName: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    conf
  }

//  private def save
}
