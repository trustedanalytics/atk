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
package org.trustedanalytics.atk.plugins.orientdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema.{ VertexSchema, DataTypes, GraphSchema, Column }

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wtaie on 4/18/16.
 */
class DataTypeToOTypeConversionTest extends WordSpec with Matchers {
  "Data Types to OType conversion" should {
    "Convert DataTypes to OrientDb OTypes" in {
      val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
      val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)

      val vertices: List[Row] = List(
        new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350)),
        new GenericRow(Array(2L, "l1", "Alice", "SFO", "SEA", 465)),
        new GenericRow(Array(3L, "l1", "Fred", "NYC", "PIT", 675)),
        new GenericRow(Array(4L, "l1", "Lucy", "LAX", "PDX", 450)))
      val oTypeBuffer = new ArrayBuffer[String]()
      val dataTypeBuffer = new ArrayBuffer[String]()
      columns.foreach(col => {
        val colName = col.name
        val colDataType = col.dataType
        val dataTypesConversion = new DataTypeToOTypeConversion
        val oColumnDataType = dataTypesConversion.convertDataTypeToOrientDbType(colDataType)
        val oTypeName = oColumnDataType
        oTypeBuffer += oTypeName.toString
        dataTypeBuffer += colDataType.toString
      })

      oTypeBuffer should contain theSameElementsAs Array("LONG", "STRING", "STRING", "STRING", "STRING", "INTEGER")

    }
  }

}
