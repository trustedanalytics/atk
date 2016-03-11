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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import com.cloudera.sparkts.models.AutoregressionX
import org.apache.spark.frame.FrameRdd
//import org.apache.spark.mllib.linalg.{ Vectors, DenseVector }
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

// ARX example
import org.apache.commons.math3.random.MersenneTwister
import breeze.linalg._
import com.cloudera.sparkts

class ARXModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  "ARXTrainArgs" should "be created with valid arguments" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val keyCol = "key"
    val timeseriesCol = "values"
    val xColumns = List("temperature", "humidity")

    val trainArgs = ARXTrainArgs(modelRef, frameRef, timeseriesCol, xColumns, 1, 1)

  }

  it should "throw an exception when the time series column name is empty or null" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val keyCol = "key"
    val xColumns = List("temperature", "humidity")

    intercept[IllegalArgumentException] {
      ARXTrainArgs(modelRef, frameRef, "", xColumns, 1, 1)
    }

    intercept[IllegalArgumentException] {
      ARXTrainArgs(modelRef, frameRef, null, xColumns, 1, 1)
    }
  }

  it should "thrown an exception when the exogenous value column list is empty or null" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val keyCol = "key"
    val timeseriesCol = "values"
    val xColumns = List()

    intercept[IllegalArgumentException] {
      ARXTrainArgs(modelRef, frameRef, timeseriesCol, xColumns, 1, 1)
    }

    intercept[IllegalArgumentException] {
      ARXTrainArgs(modelRef, frameRef, timeseriesCol, null, 1, 1)
    }
  }

  "ARXTrainPlugin" should "verifyVectorColumn() with valid arguments and return the vector length" in {

    val schema = FrameSchema(List(Column("name", DataTypes.string), Column("timeseries", DataTypes.vector(2)), Column("x", DataTypes.vector(2))))

    assertResult(2) { ARXFunctions.verifyVectorColumn(schema, "timeseries") }

    assertResult(2) { ARXFunctions.verifyVectorColumn(schema, "x") }
  }

  it should "throw an exception from verifyVectorColumns() if the column specified is not a vector" in {
    val schema = FrameSchema(List(Column("name", DataTypes.string), Column("timeseries", DataTypes.vector(2)), Column("x", DataTypes.vector(2))))

    intercept[IllegalArgumentException] {
      ARXFunctions.verifyVectorColumn(schema, "bogusColumnName")
    }

    intercept[IllegalArgumentException] {
      ARXFunctions.verifyVectorColumn(schema, "name")
    }
  }

  "ARXFunctions getYandXFromFrame" should "throw an exception if the y or x columns are not numerical" in {
    val schema = FrameSchema(List(Column("float_value", DataTypes.float32), Column("str_value", DataTypes.string), Column("int_value", DataTypes.int32)))
    val rows = sparkContext.parallelize((1 to 10).map(i => Array(i, i.toString, i)))
    val rdd = FrameRdd.toFrameRdd(schema, rows)

    intercept[IllegalArgumentException] {
      // We should get an exception when y is a string
      ARXFunctions.getYandXFromFrame(rdd, "str_value", List("int_value"))
    }

    intercept[IllegalArgumentException] {
      // We should get an exception when an x column is a string
      ARXFunctions.getYandXFromFrame(rdd, "int_value", List("float_value", "str_value"))
    }
  }

  it should "throw an exception when a column does not exist" in {
    val schema = FrameSchema(List(Column("float_value", DataTypes.float32), Column("str_value", DataTypes.string), Column("int_value", DataTypes.int32)))
    val rows = sparkContext.parallelize((1 to 10).map(i => Array(i, i.toString, i)))
    val rdd = FrameRdd.toFrameRdd(schema, rows)

    intercept[IllegalArgumentException] {
      // We should get an exception when the y column does not exist
      ARXFunctions.getYandXFromFrame(rdd, "bogus", List("int_value"))
    }

    intercept[IllegalArgumentException] {
      // We should get an exception when an x column does not exist
      ARXFunctions.getYandXFromFrame(rdd, "int_value", List("float_value", "bogus"))
    }
  }

  it should "return x and y values when columns are numerical data types" in {
    val schema = FrameSchema(List(Column("name", DataTypes.string), Column("float32_value", DataTypes.float32), Column("float64_value", DataTypes.float64), Column("int_value", DataTypes.int32)))
    val rows = sparkContext.parallelize((1 to 10).map(i => Array(i.toString, i, i * 2, i * 3)))
    val rdd = FrameRdd.toFrameRdd(schema, rows)

    val (y, x) = ARXFunctions.getYandXFromFrame(rdd, "float32_value", List("float64_value", "int_value"))
    for (i <- 1 until 10) {
      assert(y(i - 1) == i)
      assert(x(i - 1, 0) == (i * 2))
      assert(x(i - 1, 1) == (i * 3))
    }
  }

}

