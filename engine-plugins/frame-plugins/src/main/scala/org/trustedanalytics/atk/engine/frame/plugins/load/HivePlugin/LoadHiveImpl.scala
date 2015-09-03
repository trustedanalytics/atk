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

package org.trustedanalytics.atk.engine.frame.plugins.load.HivePlugin

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Column }

import scala.collection.mutable.ListBuffer

/**
 * Helper class for creating an RDD from hive
 */
object LoadHiveImpl extends Serializable {

  /**
   * Converts hive data to frame
   * @param rdd a hive rdd
   * @return a frame rdd
   */
  def convertHiveRddToFrameRdd(rdd: SchemaRDD): FrameRdd = {
    val array: Seq[StructField] = rdd.schema.fields
    val list = new ListBuffer[Column]
    for (field <- array) {
      list += new Column(field.name, FrameRdd.sparkDataTypeToSchemaDataType(field.dataType))
    }
    val schema = new FrameSchema(list.toList)
    val convertedRdd: RDD[org.apache.spark.sql.Row] = rdd.map(row => {
      val mutableRow = new GenericMutableRow(row.length)
      row.toSeq.zipWithIndex.foreach {
        case (o, i) =>
          if (o == null) {
            mutableRow(i) = null
          }
          else if (array(i).dataType.getClass == TimestampType.getClass || array(i).dataType.getClass == DateType.getClass) {
            mutableRow(i) = o.toString
          }
          else if (array(i).dataType.getClass == ShortType.getClass) {
            mutableRow(i) = row.getShort(i).toInt
          }
          else if (array(i).dataType.getClass == BooleanType.getClass) {
            mutableRow(i) = row.getBoolean(i).compareTo(false)
          }
          else if (array(i).dataType.getClass == ByteType.getClass) {
            mutableRow(i) = row.getByte(i).toInt
          }
          else if (array(i).dataType.getClass == classOf[DecimalType]) { // DecimalType.getClass return value (DecimalType$) differs from expected DecimalType
            mutableRow(i) = row.getAs[java.math.BigDecimal](i).doubleValue()
          }
          else {
            val colType = schema.columns(i).dataType
            mutableRow(i) = o.asInstanceOf[colType.ScalaType]
          }
      }
      mutableRow
    }
    )
    new FrameRdd(schema, convertedRdd)
  }

}
