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

package org.trustedanalytics.atk.engine.model.plugins.regression

import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.{ VectorUDT, DenseVector }
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType }
import org.apache.spark.sql.{ SQLContext, Row }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.scoring.models.LinearRegressionData
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class LinearRegressionModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  "LinearRegressionModel" should "create a LinearRegressionModel" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val rowArray: Array[Row] = Array(new GenericRow((Array[Any](1.0, new DenseVector(Array(16.8974, 2.693))))))
    val rdd = sparkContext.parallelize(rowArray)
    val schema = StructType(Seq(StructField("label", DoubleType, true), StructField("features", new VectorUDT, true)))
    val dataFrame = new SQLContext(sparkContext).createDataFrame(rdd, schema)

    val trainArgs = LinearRegressionTrainArgs(modelRef, frameRef, "label", List("obs1", "obs2"))
    val linReg = LinearRegressionTrainPlugin.initializeLinearRegressionModel(trainArgs)
    val linRegModel = linReg.fit(dataFrame)

    val linRegData = new LinearRegressionData(linRegModel, trainArgs.observationColumns, trainArgs.valueColumn)

    linRegData shouldBe a[LinearRegressionData]
    linRegModel shouldBe a[LinearRegressionModel]
  }

  "LinearRegressionModel" should "thow an IllegalArgumentException for empty observationColumns during train" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      LinearRegressionTrainArgs(modelRef, frameRef, "label", List())
    }
  }

  "LinearRegressionModel" should "thow an IllegalArgumentException for empty labelColumn during train" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      LinearRegressionTrainArgs(modelRef, frameRef, "", List("obs1", "obs2"))
    }
  }

}
