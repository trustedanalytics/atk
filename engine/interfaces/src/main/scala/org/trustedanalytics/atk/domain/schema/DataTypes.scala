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

package org.trustedanalytics.atk.domain.schema

import org.joda.time.DateTime
import org.trustedanalytics.atk.event.EventLogging
import org.apache.commons.lang3.StringUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{ JsValue, _ }

import scala.collection.immutable.Set
import scala.collection.mutable
import scala.util.{ Success, Try }
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.trustedanalytics.atk.domain.DomainJsonProtocol.dateTimeFormat

/**
 * Datatypes supported for frames, graphs, etc.
 */
object DataTypes extends EventLogging {

  /**
   * The datatype trait
   */
  sealed trait DataType {
    type ScalaType

    def parse(raw: Any): Try[ScalaType]

    def toScalaType(raw: Any): ScalaType = parse(raw) match {
      case Success(x) => x
      case _ => throw new RuntimeException(s"Unable to convert ${raw} to ${scalaType.getName}")
    }

    /** True if the supplied value matches this data type */
    def isType(raw: Any): Boolean

    def scalaType: Class[ScalaType]

    def typedJson(raw: Any): JsValue

    def asDouble(raw: Any): Double

    def asString(raw: Any): String = {
      raw.toString
    }

    def isNumerical: Boolean

    /** True if data type is an integral data type (e.g., int32, int64). */
    def isInteger: Boolean

    def isVector: Boolean = false

    /**
     * Looser type equality than strict object equals, to enable things like vector.equalsDataType(vector(4)) returns true
     */
    def equalsDataType(dataType: DataType): Boolean = {
      this == dataType
    }
  }

  /**
   * 32 bit ints
   */
  case object int32 extends DataType {
    override type ScalaType = Int

    override def parse(raw: Any) = Try {
      toInt(raw)
    }

    override def isType(raw: Any): Boolean = {
      // TODO: nulls aren't allowed for now but we will need to support nulls or Nones later
      raw != null && raw.isInstanceOf[Int]
    }

    override def scalaType = classOf[Int]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[Int].toJson
    }

    override def asDouble(raw: Any): Double = {
      raw.asInstanceOf[Int].toDouble
    }

    override def isNumerical = true

    override def isInteger = true
  }

  /**
   * 64 bit ints
   */
  case object int64 extends DataType {
    override type ScalaType = Long

    override def parse(raw: Any) = Try {
      toLong(raw)
    }

    override def isType(raw: Any): Boolean = {
      // TODO: nulls aren't allowed for now but we will need to support nulls or Nones later
      raw != null && raw.isInstanceOf[Long]
    }

    override def scalaType = classOf[Long]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[Long].toJson
    }

    override def asDouble(raw: Any): Double = {
      raw.asInstanceOf[Long].toDouble
    }

    override def isNumerical = true

    override def isInteger = true
  }

  /**
   * 32 bit floats
   */
  case object float32 extends DataType {
    override type ScalaType = Float

    override def parse(raw: Any) = Try {
      toFloat(raw)
    }

    override def isType(raw: Any): Boolean = {
      // TODO: nulls aren't allowed for now but we will need to support nulls or Nones later
      raw != null && raw.isInstanceOf[Float]
    }

    override def scalaType = classOf[Float]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[Float].toJson
    }

    override def asDouble(raw: Any): Double = {
      raw.asInstanceOf[Float].toDouble
    }

    override def isNumerical = true

    override def isInteger = false
  }

  /**
   * 64 bit floats
   */
  case object float64 extends DataType {

    override type ScalaType = Double

    override def parse(raw: Any) = Try {
      toDouble(raw)
    }

    override def isType(raw: Any): Boolean = {
      // TODO: nulls aren't allowed for now but we will need to support nulls or Nones later
      raw != null && raw.isInstanceOf[Double]
    }

    override def scalaType = classOf[Double]

    override def typedJson(raw: Any) = {
      raw.asInstanceOf[Double].toJson
    }

    override def asDouble(raw: Any): Double = {
      raw.asInstanceOf[Double]
    }

    override def isNumerical = true

    override def isInteger = false
  }

  /**
   * Strings
   */
  case object string extends DataType {

    override type ScalaType = String

    override def parse(raw: Any) = Try {
      toStr(raw)
    }

    override def isType(raw: Any): Boolean = {
      // where null is allowed we accept null as this type
      raw == null || raw.isInstanceOf[String]
    }

    override def scalaType = classOf[String]

    override def typedJson(raw: Any): JsValue = {
      raw.asInstanceOf[String].toJson
    }

    override def asDouble(raw: Any): Double = {
      try {
        java.lang.Double.parseDouble(raw.asInstanceOf[String])
      }
      catch {
        case e: Exception => throw new IllegalArgumentException("Could not parse " + raw + " as a Double.")
      }
    }

    override def isNumerical = false

    override def isInteger = false
  }

  /**
   * DateTime
   */
  case object datetime extends DataType {

    override type ScalaType = String

    override def scalaType = classOf[ScalaType]

    implicit def stringToDateTime(s: ScalaType): DateTime = toDateTime(s)

    override def parse(raw: Any) = Try {
      toDateTime(raw).toString
    }

    override def asString(raw: Any): String = {
      parse(raw).get.asInstanceOf[String]
    }

    def compare(valueA: DateTime, valueB: DateTime): Int = {
      valueA.compareTo(valueB)
    }

    def compare(valueA: String, valueB: String): Int = {
      toDateTime(valueA).compareTo(toDateTime(valueB))
    }

    override def isType(raw: Any): Boolean = {
      // where null is allowed we accept null as this type
      raw == null || raw.isInstanceOf[DateTime] || Try { toDateTime(raw) }.isSuccess
    }

    override def typedJson(raw: Any): JsValue = {
      asString(raw).toJson
    }

    override def asDouble(raw: Any): Double =
      throw new IllegalArgumentException("Could not parse datetime object " + raw + " as a Double.")

    override def isNumerical = false

    override def isInteger = false
  }

  /**
   * Vectors
   *
   * vector case class which represents a type with a specific length
   * @param length number of doubles in the vector
   */
  case class vector(length: Long) extends DataType {

    override type ScalaType = vector.ScalaType

    override def scalaType = classOf[vector.ScalaType]

    override def isNumerical = false

    override def isInteger = false

    override def isVector = true

    override def equalsDataType(dataType: DataType): Boolean = {
      dataType match {
        case vector(x) => x == length
        case _ => dataType.equalsDataType(this)
      }
    }

    override def parse(raw: Any) = Try {
      toVector(length)(raw)
    }

    override def isType(raw: Any): Boolean = {
      // where null is allowed we accept null as this type
      raw == null || (raw.isInstanceOf[vector.ScalaType] && raw.asInstanceOf[vector.ScalaType].length == length)
    }

    override def typedJson(raw: Any): JsValue = {
      parse(raw).get.toJson
    }

    override def asString(raw: Any): String = {
      parse(raw).get.mkString(",")
    }
    override def asDouble(raw: Any): Double = {
      require(length == 1, "Vector must be of length 1 to be cast as a double.")
      vector.asDouble(raw)
    }
  }

  /**
   * vector object for general vector type work, but not considered a DataType
   */
  case object vector {

    type ScalaType = Vector[Double]

    def parse(raw: Any) = Try {
      toVector()(raw)
    }

    /**
     * Determines if the datatype is a vector, regardless of length
     */
    def isVectorDataType(dataType: DataType): Boolean = {
      dataType match {
        case vector(x) => true
        case _ => false
      }
    }

    def typedJson(raw: Any): JsValue = {
      parse(raw).get.toJson
    }

    def asString(raw: Any): String = {
      try {
        parse(raw).get.mkString(",")
      }
      catch {
        case e: Exception => throw new IllegalArgumentException(s"Could not parse $raw as a Vector: ${e.getMessage}")
      }
    }

    def asDouble(raw: Any): Double = {
      try {
        val vec = raw.asInstanceOf[vector.ScalaType] // we'll try to convert a Vector if it only has one item in
        require(vec.size == 1, "Vector must be of size 1.")
        vec.head
      }
      catch {
        case e: Exception => throw new IllegalArgumentException(s"Could not parse $raw as a Vector: ${e.getMessage}")
      }
    }

    def compare(valueA: vector.ScalaType, valueB: vector.ScalaType): Int = {
      if (valueB == null) {
        if (valueA == null) {
          0
        }
        else {
          1
        }
      }
      else {
        var comparison: Int = valueA.size.compare(valueB.size)
        if (comparison == 0) {
          valueA.indices.takeWhile(i => {
            comparison = valueA(i).compare(valueB(i))
            comparison == 0
          })
        }
        comparison
      }
    }

  }

  /**
   * function to check if data type is type vector of any length
   * @return
   */
  def isVectorDataType: DataType => Boolean = vector.isVectorDataType

  /**
   * function to check if data type is of type integer
   * @return true if isInteger otherwise false
   */
  def isIntegerDataType: DataType => Boolean = _.isInteger

  /**
   * Checks if two data types are compatible.
   *
   * @return True, if both data types are equal, both data types are integers,
   *         or both data types are floats. False otherwise.
   */
  def isCompatibleDataType(dataType1: DataType, dataType2: DataType): Boolean = {
    dataType1 match {
      case t if dataType1.equalsDataType(dataType2) => true
      case i if dataType1.isInteger && dataType2.isInteger => true
      case d if dataType1.equalsDataType(DataTypes.float32) || dataType1.equalsDataType(DataTypes.float64) =>
        dataType2.equalsDataType(DataTypes.float32) || dataType2.equalsDataType(DataTypes.float64)
      case _ => false
    }
  }

  /**
   * This is a special type for values that should be ignored while importing from CSV file.
   *
   * Any column with this type should be dropped.
   */
  case object ignore extends DataType {

    override type ScalaType = Null

    override def parse(s: Any): Try[ignore.ScalaType] = Try {
      null
    }

    override def isType(raw: Any): Boolean = {
      // always report false - we don't every want to match this type
      false
    }

    override def scalaType = classOf[Null]

    override def typedJson(raw: Any): JsValue = {
      JsNull
    }

    override def asDouble(raw: Any): Double = {
      throw new IllegalArgumentException("cannot convert ignore type to Double")
    }

    override def isNumerical = false

    override def isInteger = false
  }

  /**
   * An alias for string
   */
  val str = string

  /**
   * An alias for string
   */
  val unicode = string

  /**
   * An alias for int32
   */
  val int = int32

  /**
   * All the supported datatypes
   */
  val supportedPrimitiveTypes: Map[String, DataType] =
    Seq(int32, int64, float32,
      float64, string, ignore, datetime)
      .map(t => t.toString -> t)
      .toMap ++
      Map("str" -> string,
        "unicode" -> string,
        "int" -> int32)

  /**
   * Determine root DataType that all DataTypes in list can be converted into.
   * @param dataTypes DataTypes to merge
   * @return  Merged DataType
   */
  def mergeTypes(dataTypes: List[DataType]): DataType = {
    dataTypes.toSet match {
      case x if x.size == 1 => x.head
      case x if Set[DataType](string).subsetOf(x) => string
      case x if Set[DataType](float64).subsetOf(x) => float64
      case x if Set[DataType](int64, float32).subsetOf(x) => float64
      case x if Set[DataType](int32, float32).subsetOf(x) => float32
      case x if Set[DataType](int32, int64).subsetOf(x) => int64
      case _ => string
    }
  }

  /**
   * Converts a string such as "int32" to a datatype if possible.
   * @param s a string that matches the name of a data type
   * @return the data type object corresponding to the name
   */
  implicit def toDataType(s: String): DataType = {
    supportedPrimitiveTypes.getOrElse(s, null) match {
      case null => toComplexDataType(s)
      case x => x
    }
  }

  private def toComplexDataType(s: String): DataType = {
    val vectorPattern = """^vector\((\d+)\)$""".r
    s match {
      case vectorPattern(length) => vector(length.toLong)
      case "vector" => DataTypes.string
      case _ => throw new IllegalArgumentException(s"Invalid datatype: '$s'")
    }
  }

  /**
   * Converts a data type to a string
   * @param d the data type object
   * @return the name of the object. For example, the int32 data type is named "int32".
   */
  implicit def toString(d: DataType): String = d.toString

  /**
   * Convert an array of strings into an array of values, using the provided schema types
   * to guide conversion.
   *
   * @param columnTypes the types of the columns. The values in the strings array are assumed to be in
   *                    the same order as the values in the columnTypes array.
   * @param values the strings to be converted
   * @return the converted values. Any values that cannot be parsed will result in an illegal argument exception.
   */
  def parseMany(columnTypes: Array[DataType])(values: Array[Any]): Array[Any] = {
    val frameColumnCount = columnTypes.length
    val dataCount = values.length

    if (frameColumnCount != dataCount)
      throw new IllegalArgumentException(s"Expected $frameColumnCount columns, but got $dataCount columns in this row.")

    val lifted = columnTypes.lift
    values.zipWithIndex.map {
      case (s, i) =>
        s match {
          case null => null
          case _ =>
            val colType = lifted(i).get
            val value = colType.parse(s)

            if (value.isSuccess || colType.isNumerical == false)
              value.get
            else
              null // return null if the parse was unsuccessful for numerical datatypes

        }
    }
  }

  /**
   * Get a String description of the data type of the supplied value
   */
  def dataTypeOfValueAsString(value: Any): String = {
    if (value == null) {
      "null"
    }
    else {
      // TODO: we should convert this to our type names
      value.getClass.getName
    }
  }

  @deprecated // Added deprecation to note this function is flawed; need a more sophisticated type sniffer (if even possible), especially as more types are added, like datetime, vector, lists, etc. which may have the same underlying raw values --blbarker 9/4/15
  def dataTypeOfValue(value: Any): DataType = {
    val matchesPrimitives = supportedPrimitiveTypes.filter { case (name: String, dataType: DataType) => dataType.isType(value) }.toList
    if (matchesPrimitives.nonEmpty) {
      if (matchesPrimitives.length > 1) {
        // this would happen with null
        warn(s"$value matched more than one type: $matchesPrimitives")
      }
      matchesPrimitives.head._2
    }
    else {
      // Check complex types
      Try {
        vector(vector.parse(value).get.length)
      }.getOrElse(throw new IllegalArgumentException("No matching data type found for value: " + value))
    }
  }

  /**
   * Attempt to cast Any type to Double
   *
   * @param value input Any type to be cast
   * @return value cast as Double, if possible
   */
  def toDouble(value: Any): Double = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Double")
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case f: Float => f.toDouble
      case d: Double => d
      case bd: BigDecimal => bd.toDouble
      case s: String => s.trim().toDouble
      case v: vector.ScalaType => vector.asDouble(v)
      case _ => throw new IllegalArgumentException(s"The following value is not a numeric data type: $value")
    }
  }

  def toBigDecimal(value: Any): BigDecimal = {
    value match {
      case null => null
      case i: Int => BigDecimal(i)
      case l: Long => BigDecimal(l)
      case f: Float => BigDecimal(f)
      case d: Double => BigDecimal(d)
      case bd: BigDecimal => bd
      case s: String => BigDecimal(s)
      case v: vector.ScalaType => BigDecimal(vector.asDouble(v))
      case _ => throw new IllegalArgumentException(s"The following value is not of numeric data type: $value")
    }
  }

  def toLong(value: Any): Long = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Long")
      case i: Int => i.toLong
      case l: Long => l
      case f: Float => f.toLong
      case d: Double => d.toLong
      case bd: BigDecimal => bd.toLong
      case s: String => s.trim().toLong
      case v: vector.ScalaType => vector.asDouble(v).toLong
      case _ => throw new RuntimeException(s"${value.getClass.getName} toLong is not implemented")
    }
  }

  def toInt(value: Any): Int = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Int")
      case i: Int => i
      case l: Long => l.toInt
      case f: Float => f.toInt
      case d: Double => d.toInt
      case bd: BigDecimal => bd.toInt
      case s: String => s.trim().toInt
      case v: vector.ScalaType => vector.asDouble(v).toInt
      case _ => throw new RuntimeException(s"${value.getClass.getName} toInt is not implemented")
    }
  }

  def toFloat(value: Any): Float = {
    value match {
      case null => throw new IllegalArgumentException("null cannot be converted to Float")
      case i: Int => i.toFloat
      case l: Long => l.toFloat
      case f: Float => f
      case d: Double => d.toFloat
      case bd: BigDecimal => bd.toFloat
      case s: String => s.trim().toFloat
      case v: vector.ScalaType => vector.asDouble(v).toFloat
      case _ => throw new RuntimeException(s"${value.getClass.getName} toFloat is not implemented")
    }
  }

  def toStr(value: Any): String = {
    value match {
      case null => null
      case s: String => s
      case v: vector.ScalaType => vector.asString(v)
      case value =>
        try {
          value.toString
        }
        catch {
          case e: Exception => throw new RuntimeException(s"${value.getClass.getName} toStr is not implemented")
        }
    }
  }

  def toVector(length: Long = -1)(value: Any): vector.ScalaType = {
    val vec = value match {
      case null => null
      case i: Int => toVector(length)(i.toDouble)
      case l: Long => toVector(length)(l.toDouble)
      case f: Float => toVector(length)(f.toDouble)
      case d: Double => Vector[Double](d)
      case bd: BigDecimal => toVector(length)(bd.toDouble)
      case s: String =>
        val jsonStr = if (s.trim.startsWith("[")) s else "[" + s + "]"
        JsonParser(jsonStr).convertTo[List[Double]].toVector
      case v: vector.ScalaType => v
      case ab: ArrayBuffer[_] => ab.map(value => toDouble(value)).toVector
      case a: Array[_] => a.map(value => toDouble(value)).toVector
      case l: List[_] => l.map(value => toDouble(value)).toVector
      case i: Iterator[_] => i.map(value => toDouble(value)).toVector
      case i: java.util.Iterator[_] => i.map(value => toDouble(value)).toVector
      case wa: mutable.WrappedArray[_] => wa.map(value => toDouble(value)).toVector
      case _ => throw new RuntimeException(s"${value.getClass.getName} toVector is not implemented")
    }
    if (length >= 0) {
      require(vec.length == length, s"Expected vector of length $length, but received one of length ${vec.length}")
    }
    vec
  }

  def toDateTime(value: Any): DateTime = {
    value match {
      case null => null
      case s: String => DateTime.parse(s) // ISO 8601
      case dt: DateTime => dt
      case _ => throw new RuntimeException(s"${value.getClass.getName} toDateTime is not implemented")
    }
  }

  /**
   * Convert de-limited string to array of big decimals
   */
  def toBigDecimalArray(value: Any, delimiter: String = ","): Array[BigDecimal] = {
    //TODO: Re-visit once we support lists
    value match {
      case null => Array.empty[BigDecimal]
      case s: String => s.split(delimiter).map(x => {
        if (StringUtils.isBlank(x)) null.asInstanceOf[BigDecimal] else toBigDecimal(x)
      })
      case _ => throw new IllegalArgumentException(s"The following value is not an array of doubles: $value")
    }
  }

  /**
   * Compare our supported data types
   * @param valueA
   * @param valueB
   * @return
   */
  def compare(valueA: Any, valueB: Any): Int = {
    if (valueB == null) {
      if (valueA == null) {
        0
      }
      else {
        1
      }
    }
    else {
      valueA match {
        case null => -1
        case i: Int => i.compare(DataTypes.toInt(valueB))
        case l: Long => l.compare(DataTypes.toLong(valueB))
        case f: Float => f.compare(DataTypes.toFloat(valueB))
        case d: Double => d.compare(DataTypes.toDouble(valueB))
        case s: String => s.compareTo(valueB.toString)
        case v: vector.ScalaType => vector.compare(v, DataTypes.toVector()(valueB))
        case dt: DateTime => dt.compareTo(DataTypes.toDateTime(valueB))
        case _ => throw new RuntimeException(s"${valueA.getClass.getName} comparison is not implemented")
      }
    }

  }

  /**
   * Match java type object and return DataType instance.
   *
   * @return DataType instance
   */
  def javaTypeToDataType(a: java.lang.Class[_]): DataType = {
    val intType = classOf[java.lang.Integer]
    val longType = classOf[java.lang.Long]
    val floatType = classOf[java.lang.Float]
    val doubleType = classOf[java.lang.Double]
    val stringType = classOf[java.lang.String]

    a match {
      case `intType` => int32
      case `longType` => int64
      case `floatType` => float32
      case `doubleType` => float64
      case `stringType` => string
      case _ => throw new IllegalArgumentException(s"unsupported type $a")
    }
  }
}
