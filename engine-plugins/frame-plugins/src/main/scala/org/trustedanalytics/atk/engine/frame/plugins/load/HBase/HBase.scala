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

package org.trustedanalytics.atk.engine.frame.plugins.load.HBase

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

object HBase extends Serializable {

  def createRdd(sc: SparkContext, tableName: String, schema: List[HBaseSchemaArgs], startTag: Option[String], endTag: Option[String]): RDD[Array[Any]] = {
    val hBaseRDD = sc.newAPIHadoopRDD(createConfig(tableName),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hBaseRDD.map {
      case (key, row) => {
        val values = for { element <- schema }
          yield getValue(row, element.columnFamily, element.columnName, element.dataType)
        values.toArray
      }
    }
  }

  private def getValue(row: Result, columnFamily: String, columnName: String, dataType: DataType): Any = {
    val value = row.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))
    if (value == null)
      null
    else
      valAsDataType(Bytes.toString(value), dataType)
  }

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

  private def createConfig(dbName: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, dbName)

    conf
  }
}
