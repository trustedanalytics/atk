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

package org.trustedanalytics.atk.engine.frame.plugins.exporthdfs

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.ExportHdfsHiveArgs
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.hive.jdbc.Utils

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Export a frame to hive table
 */
@PluginDoc(oneLine = "Write  current frame to Hive table.",
  extended = """Table must not exist in Hive. Hive does not support case sensitive table names and columns names. Hence column names with uppercase letters will be converted to lower case by Hive.
Export of Vectors is not currently supported.""")
class ExportHdfsHivePlugin extends SparkCommandPlugin[ExportHdfsHiveArgs, UnitReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_to_hive"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportHdfsHiveArgs)(implicit invocation: Invocation) = 5

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsHiveArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame
    val SEPARATOR = "."
    val tableName = StringUtils.isEmpty(EngineConfig.hiveConnectionUrl) || arguments.tableName.contains(SEPARATOR) match {
      case true => arguments.tableName
      case false =>
        val database = Utils.parseURL(EngineConfig.hiveConnectionUrl).getDbName
        s"$database$SEPARATOR${arguments.tableName}"
    }

    exportToHdfsHive(sc, frame.rdd, tableName)
  }

  /**
   * Export to a file in Hive format
   *
   * @param frameRdd input rdd containing all columns
   * @param tablename table where to store the RDD
   */
  private def exportToHdfsHive(
    sc: SparkContext,
    frameRdd: FrameRdd,
    tablename: String) {

    val df = frameRdd.toDataFrameUsingHiveContext
    df.registerTempTable("mytable")

    val beginstring = "{\"name\": \"" + tablename + "\",\"type\": \"record\",\"fields\": "
    val array = FrameRdd.schemaToAvroType(frameRdd.frameSchema).map(column => "{\"name\":\"" + column._1 + "\", \"type\":[\"null\",\"" + column._2 + "\"]}")
    val colSchema = array.mkString("[", ",", "]")
    val endstring = "}"
    val schema = beginstring + colSchema + endstring

    df.sqlContext.asInstanceOf[org.apache.spark.sql.hive.HiveContext].sql(s"CREATE TABLE " + tablename +
      s" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS AVRO TBLPROPERTIES ('avro.schema.literal'= '${schema}' ) AS SELECT * FROM mytable")
  }
}
