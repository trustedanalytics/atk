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

package org.trustedanalytics.atk.engine.frame

import org.apache.commons.csv.{ CSVPrinter, CSVFormat }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.GenericArrayData
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.domain.schema._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.graphbuilder.elements.GBVertex

import scala.collection.mutable.ArrayBuffer
import java.lang.StringBuilder
/**
 * This class wraps raw row data adding schema information - this allows for a richer easier to use API.
 *
 * Ideally, every row knows about its schema but this is inefficient when there are many rows.  As an
 * alternative, a single instance of this wrapper can be used to provide the same kind of API.
 *
 * @param schema the schema for the row
 */
class RowWrapper(override val schema: Schema) extends AbstractRow with Serializable {
  require(schema != null, "schema is required")

  private lazy val structType = FrameRdd.schemaToStructType(schema)

  @transient override var row: Row = null

  /**
   * Set the data in this wrapper
   *
   * @param row the data to set inside this Wrapper
   * @return this instance
   */
  def apply(row: Row): RowWrapper = {
    this.row = row
    this
  }

  def apply(internalRow: InternalRow) = {
    this.row = new GenericRow(internalRow.toSeq(structType).toArray)
    this
  }
}

/**
 * Most implementation belongs here so it can be shared with AbstractVertex and AbstractEdge
 */
trait AbstractRow {

  val schema: Schema
  var row: Row

  /**
   * Determine whether the property exists
   *
   * @param name name of the property
   * @return boolean value indicating whether the property exists
   */
  def hasProperty(name: String): Boolean = {
    try {
      schema.columnIndex(name)
      true
    }
    catch {
      case e: Exception => false
    }
  }

  /**
   * Get property
   *
   * @param columnName name of the property
   * @return property value
   */
  def value(columnName: String): Any = row(schema.columnIndex(columnName))

  /**
   * Get more than one value as a List
   *
   * @param columnNames the columns to get values for
   * @return the values for the columns
   */
  def values(columnNames: List[String] = schema.columnNames): List[Any] = {
    columnNames.map(columnName => value(columnName))
  }

  /**
   * Get property of boolean data type
   *
   * @param columnName name of the property
   * @return property value
   */
  def booleanValue(columnName: String): Boolean = row(schema.columnIndex(columnName)).asInstanceOf[Boolean]

  /**
   * Get property of integer data type
   *
   * @param columnName name of the property
   * @return property value
   */
  def intValue(columnName: String): Int = DataTypes.toInt(row(schema.columnIndex(columnName)))

  /**
   * Get property of long data type
   *
   * @param columnName name of the property
   * @return property value
   */
  def longValue(columnName: String): Long = DataTypes.toLong(row(schema.columnIndex(columnName)))

  /**
   * Get property of float data type
   *
   * @param columnName name of the property
   * @return property value
   */
  def floatValue(columnName: String): Float = DataTypes.toFloat(row(schema.columnIndex(columnName)))

  /**
   * Get property of double data type
   *
   * @param columnName name of the property
   * @return property value
   */
  def doubleValue(columnName: String): Double = DataTypes.toDouble(row(schema.columnIndex(columnName)))

  /**
   * Get property of string data type
   *
   * @param columnName name of the property
   * @return property value
   */
  def stringValue(columnName: String): String = DataTypes.toStr(row(schema.columnIndex(columnName)))

  /**
   * Get property of string data type
   *
   * @param columnName name of the property
   * @return property value
   */
  def vectorValue(columnName: String): Vector[Double] = {
    row(schema.columnIndex(columnName)) match {
      case ga: GenericArrayData => ga.toDoubleArray().toVector
      case value => DataTypes.toVector()(value)
    }
  }

  /**
   * True if value for this column is null.
   *
   * (It is non-intuitive but SparkSQL seems to allow null primitives).
   */
  def isNull(columnName: String): Boolean = row.isNullAt(schema.columnIndex(columnName))

  /**
   * Set a value in a column - validates the supplied value is the correct type
   *
   * @param name the name of the column to set
   * @param value the value of the column
   */
  def setValue(name: String, value: Any): Row = {
    validate(name, value)
    setValueIgnoreType(name, value)
  }

  /**
   * Set all of the values for an entire row with validation
   *
   * @param values the values to set
   * @return the row
   */
  def setValues(values: Array[Any]): Row = {
    validate(values)
    setValuesIgnoreType(values)
  }

  /**
   * Validate the supplied value matches the schema for the supplied columnName.
   *
   * @param name column name
   * @param value the value to check
   */
  private def validate(name: String, value: Any): Unit = {
    if (!schema.columnDataType(name).isType(value)) {
      val dataType = DataTypes.dataTypeOfValueAsString(value)
      throw new IllegalArgumentException(s"setting property $name with value $value with an incorrect data type: $dataType")
    }
  }

  private def validate(values: Array[Any]): Unit = {
    require(values.length == schema.columns.length, "number of values must match the number of columns")
    values.zip(schema.columns).foreach { case (value, column) => validate(column.name, value) }
  }

  /**
   * Set the value in a column - don't validate the type
   *
   * @param name the name of the column to set
   * @param value the value of the column
   */
  private def setValueIgnoreType(name: String, value: Any): Row = {
    val position = schema.columnIndex(name)
    val content = row.toSeq.toArray
    content(position) = value
    //TODO: what is the right way to introduce GenericMutableRow?
    row = new GenericRow(content)
    row
  }

  /**
   * Set all of the values for a row - don't validate type
   *
   * @param values all of the values
   * @return the row
   */
  private def setValuesIgnoreType(values: Array[Any]): Row = {
    //TODO: what is the right way to introduce GenericMutableRow?
    row = new GenericRow(values)
    row
  }

  /**
   * Add a property onto the end of this row.
   *
   * Since this property isn't part of the current schema, no name is supplied.
   *
   * This method changes the schema of the underlying row.
   *
   * @param value the value of the new column
   * @return the row (with a different schema)
   */
  def addValue(value: Any): Row = {
    val content = row.toSeq.toArray :+ value
    //TODO: what is the right way to introduce GenericMutableRow?
    row = new GenericRow(content)
    row
  }

  /**
   * Add the value if the column name doesn't exist, otherwise set the existing column
   *
   * Note this method may change the schema of the underlying row
   */
  def addOrSetValue(name: String, value: Any): Row = {
    if (!hasProperty(name)) {
      addValue(value)
    }
    else {
      setValueIgnoreType(name, value)
    }
  }

  /**
   * Convert the supplied column from the current type to the supplied dataType
   *
   * This method changes the schema of the underlying row.
   *
   * @param columnName column to change
   * @param dataType new data type to convert existing values to
   * @return the modified row (with a different schema)
   */
  def convertType(columnName: String, dataType: DataType): Row = {
    setValueIgnoreType(columnName, dataType.toScalaType(value(columnName)))
  }

  /**
   * Get underlying data for this row
   *
   * @return the actual row
   */
  def data: Row = row

  /**
   * Create a new row from the data of the columns supplied
   */
  def valuesAsRow(columnNames: Seq[String] = schema.columnNames): Row = {
    val content = valuesAsArray(columnNames)
    new GenericRow(content)
  }

  /**
   * Select several property values from their names
   *
   * @param names the names of the properties to put into an array
   * @param flattenInputs If true, flatten vector data types
   * @return values for the supplied properties
   */
  def valuesAsArray(names: Seq[String] = schema.columnNames, flattenInputs: Boolean = false): Array[Any] = {
    val arrayBuf = new ArrayBuffer[Any]()

    schema.columnIndices(names).map(i => {
      schema.column(i).dataType match {
        case DataTypes.vector(length) => if (flattenInputs) arrayBuf ++= DataTypes.toVector(length)(row(i)) else arrayBuf += row(i)
        case _ => arrayBuf += row(i)
      }
    })

    arrayBuf.toArray
  }

  /**
   * Select several property values from their names as an array of doubles
   *
   * @param names the names of the properties to put into an array
   * @param flattenInputs If true, flatten vector data types
   * @return array of doubles with values for the supplied properties
   */
  def valuesAsDoubleArray(names: Seq[String] = schema.columnNames, flattenInputs: Boolean = false): Array[Double] = {
    valuesAsArray(names, flattenInputs).map(value => DataTypes.toDouble(value))
  }

  /**
   * Values of the row as a Seq[Any]
   */
  def toSeq: Seq[Any] = {
    row.toSeq
  }

  /**
   * Values of the row as an Array[Any]
   */
  def toArray: Array[Any] = {
    row.toSeq.toArray
  }

  /**
   * Create a new row matching the supplied schema adding/dropping columns as needed.
   *
   * @param updatedSchema the new schema to match
   * @return the row matching the new schema
   */
  def valuesForSchema(updatedSchema: Schema): Row = {
    val content = new Array[Any](updatedSchema.columns.length)
    for (columnName <- updatedSchema.columnNames) {
      if (columnName == GraphSchema.labelProperty && updatedSchema.isInstanceOf[GraphElementSchema]) {
        content(updatedSchema.columnIndex(columnName)) = updatedSchema.asInstanceOf[GraphElementSchema].label
      }
      else if (schema.hasColumnWithType(columnName, updatedSchema.columnDataType(columnName))) {
        content(updatedSchema.columnIndex(columnName)) = value(columnName)
      }
      else if (schema.hasColumn(columnName)) {
        val dataType = updatedSchema.columnDataType(columnName)
        content(updatedSchema.columnIndex(columnName)) = dataType.toScalaType(value(columnName))
      }
      else {
        // it is non-intuitive but even primitives can be null with Rows
        content(updatedSchema.columnIndex(columnName)) = null
      }
    }
    new GenericRow(content)
  }

  /**
   * Create a new empty row
   */
  def create(): Row = {
    //TODO: what is the right way to introduce GenericMutableRow?
    val content = new Array[Any](schema.columns.length)
    row = new GenericRow(content)
    row
  }

  /**
   * Create a row with values
   *
   * @param content the values
   * @return the row
   */
  def create(content: Array[Any]): Row = {
    create()
    setValues(content)
  }

  def create(vertex: GBVertex): Row = {
    create()
    vertex.properties.foreach(prop => setValue(prop.key, prop.value))
    row
  }

  /**
   * Export row to CSV format
   *
   * @param csvFormat
   * @return CSV record/row
   */
  def exportRowToCsv(csvFormat: CSVFormat): String = {
    val stringBuilder = new StringBuilder
    val printer = new CSVPrinter(stringBuilder, csvFormat)
    val array = row.toSeq.map(col =>
      col match {
        case null => ""
        case arr: ArrayBuffer[_] => arr.mkString(",")
        case seq: Seq[_] => seq.mkString(",")
        case x => x.toString
      })
    for (i <- array) printer.print(i)
    stringBuilder.toString
  }

}
