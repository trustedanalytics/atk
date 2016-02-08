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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{ Vectors, DenseVector }
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARXTrainPlugin
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class ARXModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  "ARXTrainArgs" should "be created with valid arguments" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val timeseriesCol = "values"
    val xColumns = List("temperature", "humidity")

    val trainArgs = ARXTrainArgs(modelRef, frameRef, timeseriesCol, xColumns, 1, 1)

  }

  it should "throw an exception when the time series column name is empty or null" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
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
    val rows = sparkContext.parallelize((1 to 10).map(i => Array(i.toString, Array(5.0, 1.0), Array(2.0, 3.5))))
    val schema = FrameSchema(List(Column("name", DataTypes.string), Column("timeseries", DataTypes.vector(2)), Column("x", DataTypes.vector(2))))
    val rdd = FrameRdd.toFrameRdd(schema, rows.asInstanceOf[RDD[Array[Any]]])

    assertResult(2) { ARXTrainPlugin.verifyVectorColumn(rdd, "timeseries", "timeseriesColumn") }

    assertResult(2) { ARXTrainPlugin.verifyVectorColumn(rdd, "x", "xColumn") }
  }

  it should "throw an exception from verifyVectorColumns() if the column specified is not a vector" in {
    val rows = sparkContext.parallelize((1 to 10).map(i => Array(i.toString, Array(5.0, 1.0), Array(2.0, 3.5))))
    val schema = FrameSchema(List(Column("name", DataTypes.string), Column("timeseries", DataTypes.vector(2)), Column("x", DataTypes.vector(2))))
    val rdd = FrameRdd.toFrameRdd(schema, rows.asInstanceOf[RDD[Array[Any]]])

    intercept[IllegalArgumentException] {
      ARXTrainPlugin.verifyVectorColumn(rdd, "bogusColumnName", "bogusColumn")
    }

    intercept[IllegalArgumentException] {
      ARXTrainPlugin.verifyVectorColumn(rdd, "name", "nameColumm")
    }
  }

}
