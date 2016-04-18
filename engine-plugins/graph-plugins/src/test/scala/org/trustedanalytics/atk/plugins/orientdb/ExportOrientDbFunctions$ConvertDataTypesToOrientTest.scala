package org.trustedanalytics.atk.plugins.orientdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ WordSpec, Matchers }
import org.trustedanalytics.atk.domain.schema.{ VertexSchema, DataTypes, GraphSchema, Column }

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wtaie on 4/12/16.
 */
class ExportOrientDbFunctions$ConvertDataTypesToOrientTest extends WordSpec with Matchers {
  "ExportOrientDbFunctions" should {
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
        val oColumnDataType = ExportOrientDbFunctions.DataTypesToOTypeMatch.convertDataTypeToOrientDbType(colDataType)
        val oTypeName = oColumnDataType
        oTypeBuffer += oTypeName.toString
        dataTypeBuffer += colDataType.toString
      })

      oTypeBuffer should contain theSameElementsAs Array("LONG", "STRING", "STRING", "STRING", "STRING", "INTEGER")

    }
  }
}
