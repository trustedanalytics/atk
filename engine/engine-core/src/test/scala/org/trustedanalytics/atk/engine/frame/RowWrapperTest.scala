package org.trustedanalytics.atk.engine.frame

import org.apache.commons.csv.CSVFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
/**
 * Created by wtaie on 3/23/16.
 */
class RowWrapperTest extends WordSpec with Matchers {

  val inputRows: Array[Row] = Array(
    new GenericRow(Array[Any]("a", 1, 1d, "w")),
    new GenericRow(Array[Any]("c", 5, 1d, "5")))

  val inputSchema = FrameSchema(List(
    Column("col_0", DataTypes.string),
    Column("col_1", DataTypes.int32),
    Column("col_2", DataTypes.float64),
    Column("col_3", DataTypes.string)
  ))
  "export row to CSV" should {
    "convert input rows to string" in {
      val csvFormat = CSVFormat.RFC4180.withDelimiter(',')
      val rowWrapper1 = new RowWrapper(inputSchema)
      val csvRecords = rowWrapper1(inputRows(1)).exportRowToCsv(csvFormat)
      csvRecords shouldEqual ("c,5,1.0,5")
    }
    "throw an IllegalArgumentException if input rows were empty" in {
      inputRows should not be empty
    }
  }

}
