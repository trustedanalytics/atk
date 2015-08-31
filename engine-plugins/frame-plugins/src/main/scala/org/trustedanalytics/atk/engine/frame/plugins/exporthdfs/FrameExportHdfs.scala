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

import java.io.{ PrintWriter, File }

import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.frame.MiscFrameFunctions
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.commons.csv.{ CSVPrinter, CSVFormat }

import scala.collection.mutable.ArrayBuffer

/**
 * Object for exporting frames to files
 */

object FrameExportHdfs extends Serializable {

  /**
   * Export to a file in CSV format
   *
   * @param frameRdd input rdd containing all columns
   * @param filename file path where to store the file
   */
  def exportToHdfsCsv(
    frameRdd: FrameRdd,
    filename: String,
    separator: Char,
    count: Option[Int] = None,
    offset: Option[Int] = None) {

    val recCount = count.getOrElse(-1)
    val recOffset = offset.getOrElse(0)

    val filterRdd = if (recCount > 0) MiscFrameFunctions.getPagedRdd(frameRdd, recOffset, recCount, -1) else frameRdd
    val headers = frameRdd.frameSchema.columnNames.mkString(separator.toString)
    val csvFormat = CSVFormat.RFC4180.withDelimiter(separator)

    val csvRdd = filterRdd.map(row => {
      val stringBuilder = new java.lang.StringBuilder
      val printer = new CSVPrinter(stringBuilder, csvFormat)
      val array = row.toSeq.map(col => if (col == null) "" else {
        if (col.isInstanceOf[ArrayBuffer[_]]) {
          col.asInstanceOf[ArrayBuffer[Double]].mkString(",")
        }
        else {
          col.toString
        }
      })
      for (i <- array) printer.print(i)
      stringBuilder.toString
    })

    val addHeaders = frameRdd.sparkContext.parallelize(List(headers)) ++ csvRdd
    addHeaders.saveAsTextFile(filename)
  }

  /**
   * Export to a file in JSON format
   *
   * @param frameRdd input rdd containing all columns
   * @param filename file path where to store the file
   */
  def exportToHdfsJson(
    frameRdd: FrameRdd,
    filename: String,
    count: Option[Int],
    offset: Option[Int]) {

    val recCount = count.getOrElse(-1)
    val recOffset = offset.getOrElse(0)

    val filterRdd = if (recCount > 0) MiscFrameFunctions.getPagedRdd(frameRdd, recOffset, recCount, -1) else frameRdd
    val headers = frameRdd.frameSchema.columnNames
    val jsonRDD = filterRdd.map {
      row =>
        {
          val value = row.toSeq.zip(headers).map {
            case (k, v) => new String("\"" + v.toString + "\":" + (if (k == null) "null"
            else if (k.isInstanceOf[String]) { "\"" + k.toString + "\"" }
            else if (k.isInstanceOf[ArrayBuffer[_]]) { k.asInstanceOf[ArrayBuffer[Double]].mkString("[", ",", "]") }
            else k.toString)
            )
          }
          value.mkString("{", ",", "}")
        }
    }
    jsonRDD.saveAsTextFile(filename)
  }

  /**
   * Export to a file in Hive format
   *
   * @param frameRdd input rdd containing all columns
   * @param tablename table where to store the RDD
   */
  def exportToHdfsHive(
    sc: SparkContext,
    frameRdd: FrameRdd,
    tablename: String) {

    val df = frameRdd.toDataFrameUsingHiveContext
    df.registerTempTable("mytable")

    val beginstring = "{\"name\": \"" + tablename + "\",\"type\": \"record\",\"fields\": "
    val array = FrameRdd.schemaToHiveType(frameRdd.frameSchema).map(column => "{\"name\":\"" + column._1 + "\", \"type\":[\"null\",\"" + column._2 + "\"]}")
    val colSchema = array.mkString("[", ",", "]")
    val endstring = "}"
    val schema = beginstring + colSchema + endstring

    df.sqlContext.asInstanceOf[org.apache.spark.sql.hive.HiveContext].sql(s"CREATE TABLE " + tablename +
      s" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS AVRO TBLPROPERTIES ('avro.schema.literal'= '${schema}' ) AS SELECT * FROM mytable")
  }

}
