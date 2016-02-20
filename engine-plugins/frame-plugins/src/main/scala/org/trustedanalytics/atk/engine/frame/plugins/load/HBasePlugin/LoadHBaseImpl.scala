/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.frame.plugins.load.HBasePlugin

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{ HBaseConfiguration }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.frame.load.HBaseSchemaArgs
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

/**
 * Helper class for creating an RDD from hBase
 */
object LoadHBaseImpl extends Serializable {

  /**
   *
   * @param sc default spark context
   * @param tableName hBase table to read from
   * @param schema hBase schema for the table above
   * @param startTag optional start tag to filter the database rows
   * @param endTag optional end tag to filter the database rows
   * @return an RDD of converted hBase values
   */
  def createRdd(sc: SparkContext, tableName: String, schema: List[HBaseSchemaArgs], startTag: Option[String], endTag: Option[String]): RDD[Array[Any]] = {
    val hBaseRDD = sc.newAPIHadoopRDD(createConfig(tableName, startTag, endTag),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    hbaseRddToRdd(hBaseRDD, schema)
  }

  def hbaseRddToRdd(hbaseRDD: RDD[(ImmutableBytesWritable, Result)], schema: List[HBaseSchemaArgs]) =
    hbaseRDD.map {
      case (key, row) => {
        val values = for { element <- schema }
          yield getValue(row, element.columnFamily, element.columnName, element.dataType)

        values.toArray
      }
    }

  /**
   * Get value for cell
   * @param row hBase data
   * @param columnFamily hBase column family
   * @param columnName hBase column name
   * @param dataType internal data type of the cell
   * @return the value for the cell as per the specified datatype
   */
  private def getValue(row: Result, columnFamily: String, columnName: String, dataType: DataType): Any = {
    val value = row.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))
    if (value == null)
      null
    else
      valAsDataType(Bytes.toString(value), dataType)
  }

  /**
   * Convert value from string representation to datatype
   * @param value value as string
   * @param dataType data type to convert to
   * @return the converted value
   */
  private def valAsDataType(value: String, dataType: DataType): Any = {
    if (DataTypes.int32.equals(dataType)) {
      DataTypes.toInt(value)
    }
    else if (DataTypes.int64.equals(dataType)) {
      DataTypes.toLong(value)
    }
    else if (DataTypes.float32.equals(dataType)) {
      DataTypes.toFloat(value)
    }
    else if (DataTypes.float64.equals(dataType)) {
      DataTypes.toDouble(value)
    }
    else if (DataTypes.string.equals(dataType)) {
      value
    }
    else {
      throw new IllegalArgumentException(s"unsupported export type ${dataType.toString}")
    }
  }

  /**
   * Create initial configuration for bHase reader
   * @param tableName name of hBase table
   * @return hBase configuration
   */
  private def createConfig(tableName: String, startTag: Option[String], endTag: Option[String]): Configuration = {
    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    if (startTag.isDefined) {
      conf.set(TableInputFormat.SCAN_ROW_START, startTag.get)
    }
    if (endTag.isDefined) {
      conf.set(TableInputFormat.SCAN_ROW_STOP, endTag.get)
    }

    conf
  }
}
