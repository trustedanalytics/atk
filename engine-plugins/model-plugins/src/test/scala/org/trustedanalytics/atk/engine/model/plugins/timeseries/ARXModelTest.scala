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

import com.cloudera.sparkts.AutoregressionX
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

  val rand = new MersenneTwister(10L)
  val nRows = 50
  val nCols = 2
  val X = Array.fill(nRows, nCols)(rand.nextGaussian())
  val intercept = rand.nextGaussian * 10

  "ARXModel" should "fit model" in {
    val xCoeffs = Array(0.8, 0.2)
    val rawY = X.map(_.zip(xCoeffs).map { case (b, v) => b * v }.sum + intercept)
    val arCoeff = 0.4
    val y = rawY.scanLeft(0.0) { case (priorY, currY) => currY + priorY * arCoeff }.tail
    val dy = new DenseVector(y)
    val dx = new DenseMatrix(rows = X.length, cols = X.head.length, data = X.transpose.flatten)
    //    val xArray = Array.ofDim[Double](2, 5)
    //    xArray(0) = Array(1.0, 2.0, 1.0, 1.0, 2.0)
    //    xArray(1) = Array(5.0, 5.5, 4.0, 5.5, 5.0)
    //    val yArray = Array(10.0, 11.0, 12.0, 15.0, 10.0)
    //    val dy = new DenseVector(yArray)
    //    val dx = new DenseMatrix(rows = xArray.length, cols = xArray.head.length, data = xArray.transpose.flatten)
    val model = AutoregressionX.fitModel(dy, dx, 1, 0)
    val combinedCoeffs = Array(arCoeff) ++ xCoeffs

    model.c should be(intercept +- 1e-4)
    for (i <- combinedCoeffs.indices) {
      model.coefficients(i) should be(combinedCoeffs(i) +- 1e-4)
    }

    it should "fit the model " in {

    }
  }

}

