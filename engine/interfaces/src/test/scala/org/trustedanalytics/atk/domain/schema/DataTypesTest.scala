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

package org.trustedanalytics.atk.domain.schema

import org.trustedanalytics.atk.domain.schema.DataTypes.{ float64, float32, int64, int32 }
import org.scalatest.{ FlatSpec, Matchers }
import scala.collection.mutable.ArrayBuffer

class DataTypesTest extends FlatSpec with Matchers {

  "List[DataTypes]" should "determine which type they will combine into" in {
    DataTypes.mergeTypes(DataTypes.string :: DataTypes.int32 :: DataTypes.float64 :: Nil) should be(DataTypes.string)
    DataTypes.mergeTypes(DataTypes.string :: DataTypes.float64 :: Nil) should be(DataTypes.string)
    DataTypes.mergeTypes(DataTypes.string :: DataTypes.int64 :: DataTypes.float64 :: Nil) should be(DataTypes.string)
    DataTypes.mergeTypes(DataTypes.int32 :: DataTypes.float64 :: Nil) should be(DataTypes.float64)
    DataTypes.mergeTypes(DataTypes.int64 :: DataTypes.float32 :: Nil) should be(DataTypes.float64)
    DataTypes.mergeTypes(DataTypes.int32 :: DataTypes.int64 :: Nil) should be(DataTypes.int64)
    DataTypes.mergeTypes(DataTypes.int32 :: DataTypes.float32 :: Nil) should be(DataTypes.float32)
    DataTypes.mergeTypes(DataTypes.int32 :: DataTypes.int32 :: Nil) should be(DataTypes.int32)
  }

  "toBigDecimal" should "convert int value" in {
    val value = 100
    val bigDecimalVal = DataTypes.toBigDecimal(value)
    bigDecimalVal.intValue shouldBe value
  }

  "toBigDecimal" should "convert long value" in {
    val value: Long = 100
    val bigDecimalVal = DataTypes.toBigDecimal(value)
    bigDecimalVal.longValue shouldBe value
  }

  "toBigDecimal" should "convert float value" in {
    val value = 100.05f
    val bigDecimalVal = DataTypes.toBigDecimal(value)
    bigDecimalVal.floatValue shouldBe value
  }

  "toBigDecimal" should "convert double value" in {
    val value = 100.05
    val bigDecimalVal = DataTypes.toBigDecimal(value)
    bigDecimalVal.doubleValue shouldBe value
  }

  "toBigDecimal" should "throw Exception when non-numeric value is passed in" in {
    val value = "non-numeric"
    intercept[IllegalArgumentException] {
      DataTypes.toBigDecimal(value)
    }
  }

  "vector.asDouble" should "convert a Vector of size 1 to a double" in {
    val v = Vector[Double](2.5)
    DataTypes.vector.asDouble(v) shouldBe 2.5
  }

  "vector.asDouble" should "throw Exception when Vector size != 1" in {
    val vFat = Vector[Double](2.5, 3.6, 4.7)
    intercept[IllegalArgumentException] {
      DataTypes.vector.asDouble(vFat)
    }
    val vThin = Vector[Double]()
    intercept[IllegalArgumentException] {
      DataTypes.vector.asDouble(vThin)
    }
  }
  "vector.typeJson" should "produce good Json" in {
    val v = Vector[Double](2.5, 3.6, 4.7)
    DataTypes.vector.typedJson(v).toString shouldBe "[2.5,3.6,4.7]"
  }
  "vector" should "be a supported primative type" in {
    DataTypes.supportedPrimativeTypes.contains("vector") shouldBe false
  }

  "vector" should "compare with other vectors appropriately" in {
    val v = Vector[Double](2.5, 3.6, 4.7)
    val vGood = Vector[Double](2.5, 3.6, 4.7)
    val vTooShort = Vector[Double](2.5, 3.6)
    val vTooLong = Vector[Double](2.5, 3.6, 4.7, 5.8)
    val vBad0 = Vector[Double](2.8, 3.6, 4.7)
    val vBad1 = Vector[Double](2.5, 2.6, 4.7)
    val vBad2 = Vector[Double](2.8, 3.6, 4.8)
    DataTypes.compare(v, vGood) shouldBe 0
    DataTypes.compare(v, vTooShort) shouldBe 1
    DataTypes.compare(v, vTooLong) shouldBe -1
    DataTypes.compare(v, vBad0) shouldBe -1
    DataTypes.compare(v, vBad1) shouldBe 1
    DataTypes.compare(v, vBad2) shouldBe -1
  }

  "toVector" should "produce vectors" in {
    DataTypes.toVector()(null) shouldBe null
    DataTypes.toVector()(25) shouldBe Vector[Double](25)
    DataTypes.toVector()(123456789L) shouldBe Vector[Double](123456789L)
    DataTypes.toVector(1)(123456789L) shouldBe Vector[Double](123456789L)
    DataTypes.toVector()(3.14F) shouldBe Vector[Double](3.14F)
    DataTypes.toVector()(3.14159) shouldBe Vector[Double](3.14159)
    DataTypes.toVector()(BigDecimal(867.5309)) shouldBe Vector[Double](867.5309)
    DataTypes.toVector()("[1.2, 3.4, 5.6, 7.7,9]") shouldBe Vector[Double](1.2, 3.4, 5.6, 7.7, 9)
    DataTypes.toVector()("  [1.2, 3.4, 5.6, 7.7,9]") shouldBe Vector[Double](1.2, 3.4, 5.6, 7.7, 9)
    DataTypes.toVector(5)("  [1.2, 3.4, 5.6, 7.7,9]") shouldBe Vector[Double](1.2, 3.4, 5.6, 7.7, 9)
    DataTypes.toVector()("1.2, 777") shouldBe Vector[Double](1.2, 777)
    DataTypes.toVector()("1.2,777") shouldBe Vector[Double](1.2, 777)
    DataTypes.toVector()(Vector[Double](9.9, 8)) shouldBe Vector[Double](9.9, 8)
    DataTypes.toVector()(ArrayBuffer[Double](9.9, 8)) shouldBe Vector[Double](9.9, 8)
    DataTypes.toVector()(List[Double](9.9, 8)) shouldBe Vector[Double](9.9, 8)
    DataTypes.toVector(2)(List[Double](9.9, 8)) shouldBe Vector[Double](9.9, 8)
  }

  "asString" should "handle vectors" in {
    DataTypes.vector.asString(Vector[Double](3.14, 99)) shouldBe "3.14,99.0"
  }

  "javaTypeToDataType" should "get type from java type object" in {
    DataTypes.javaTypeToDataType(new java.lang.Integer(3).getClass) shouldBe int32
    DataTypes.javaTypeToDataType(new java.lang.Long(3).getClass) shouldBe int64
    DataTypes.javaTypeToDataType(new java.lang.Float(3.0).getClass) shouldBe float32
    DataTypes.javaTypeToDataType(new java.lang.Double(3).getClass) shouldBe float64
  }

  "string toDouble" should "fail with nice error message when given bad string" in {
    try {
      DataTypes.toDouble("badString")
      fail()
    }
    catch {
      case e: Exception =>
        e.getMessage shouldBe "For input string: \"badString\""
    }
  }

}
