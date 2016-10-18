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

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec
import org.apache.spark.frame.FrameRdd
import org.apache.spark.h2o.{ H2oModelData, H2OFrame }
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class H2oRandomForestRegressorTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  val data: Array[Row] = Array(
    Row(19.8446136104, 2.2985856384, 1),
    Row(16.8973559126, 2.6933495054, 1),
    Row(5.5548729596, 2.7777687995, 1),
    Row(46.1810010826, 3.1611961917, 0),
    Row(44.3117586448, 3.3458963222, 0),
    Row(34.6334526911, 3.6429838715, 0)
  )

  val schema = FrameSchema(List(
    Column("obs1", DataTypes.float64),
    Column("obs2", DataTypes.float64),
    Column("label", DataTypes.float64)
  ))

  val epsilon = 1e-6
  val pojoKey = "DRF_model_1476761920306_1"
  val pojo = """
               import java.util.Map;
               import hex.genmodel.GenModel;
               import hex.genmodel.annotations.ModelPojo;
               
               @ModelPojo(name="DRF_model_1476761920306_1", algorithm="atkdrf")
               public class DRF_model_1476761920306_1 extends GenModel {
                 public hex.ModelCategory getModelCategory() { return hex.ModelCategory.Regression; }
               
                 public boolean isSupervised() { return true; }
                 public int nfeatures() { return 2; }
                 public int nclasses() { return 1; }
               
                 // Names of columns used by model.
                 public static final String[] NAMES = NamesHolder_DRF_model_1476761920306_1.VALUES;
               
                 // Column domains. The last array contains domain of response column.
                 public static final String[][] DOMAINS = new String[][] {
                   /* obs1 */ null,
                   /* obs2 */ null,
                   /* label */ null
                 };
                 // Prior class distribution
                 public static final double[] PRIOR_CLASS_DISTRIB = {1.0};
                 // Class distribution used for model building
                 public static final double[] MODEL_CLASS_DISTRIB = {1.0};
               
                 public DRF_model_1476761920306_1() { super(NAMES,DOMAINS); }
                 public String getUUID() { return Long.toString(-3769836060600956926L); }
               
                 // Pass in data in a double[], pre-aligned to the Model's requirements.
                 // Jam predictions into the preds[] array; preds[0] is reserved for the
                 // main prediction (class for classifiers or value for regression),
                 // and remaining columns hold a probability distribution for classifiers.
                 public final double[] score0( double[] data, double[] preds ) {
                   java.util.Arrays.fill(preds,0);
                   DRF_model_1476761920306_1_Forest_0.score0(data,preds);
                   preds[0] /= 1;
                   return preds;
                 }
               }
               // The class representing training column names
               class NamesHolder_DRF_model_1476761920306_1 implements java.io.Serializable {
                 public static final String[] VALUES = new String[2];
                 static {
                   NamesHolder_DRF_model_1476761920306_1_0.fill(VALUES);
                 }
                 static final class NamesHolder_DRF_model_1476761920306_1_0 implements java.io.Serializable {
                   static final void fill(String[] sa) {
                     sa[0] = "obs1";
                     sa[1] = "obs2";
                   }
                 }
               }
               
               class DRF_model_1476761920306_1_Forest_0 {
                 public static void score0(double[] fdata, double[] preds) {
                   preds[0] += DRF_model_1476761920306_1_Tree_0_class_0.score0(fdata);
                 }
               }
               class DRF_model_1476761920306_1_Tree_0_class_0 {
                 static final double score0(double[] data) {
                   double pred =      (data[0] <27.236689f ? 
                       1.0f : 
                       0.0f);
                   return pred;
                 } // constant pool size = 6B, number of visited nodes = 1, static init size = 0B
               }
               """

  "H2oRandomForestRegressorTest" should "initialize random forest parameters" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val h2oFrame = mock[H2OFrame]
    when(h2oFrame.names()).thenReturn(Array[String]("obs1", "obs2", "label"))

    val trainArgs = H2oRandomForestRegressorTrainArgs(modelRef, frameRef, "label", List("obs1", "obs2"), 1, 4, 100, 2, "sqrt", Some(0), None)
    val drfParams = trainArgs.getDrfParameters(h2oFrame)

    assert(drfParams._response_column == "label")
    assert(drfParams._ignored_columns == null)
    assert(drfParams._ntrees == 1)
    assert(drfParams._mtries == 2)
    assert(drfParams._nbins == 100)
    assert(drfParams._min_rows == 2)
    assert(drfParams._seed == 0)

  }

  "H2oRandomForestRegressorTest" should "thow an IllegalArgumentException for empty observationColumns" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      H2oRandomForestRegressorTrainArgs(modelRef, frameRef, "label", List(), 1, 4, 100, 2, "sqrt", Some(0), None)
    }
  }

  "H2oRandomForestRegressorTrainArgs" should "thow an IllegalArgumentException for empty labelColumn" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      H2oRandomForestRegressorTrainArgs(modelRef, frameRef, "", List("obs1", "obs2"), 1, 4, 100, 2, "sqrt", Some(0), None)
    }
  }

  "H2oRandomForestRegressorTrainArgs" should "calculate number of features per tree" in {

    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]

    val labelCol = "label"
    val obsCols = List("obs1", "obs2", "obs3", "obs4", "obs5")

    val autoFeatures = H2oRandomForestRegressorTrainArgs(modelRef, frameRef, labelCol, obsCols, featureSubsetCategory = "auto")
    assert(autoFeatures.getMtries == -1)

    val allFeatures = H2oRandomForestRegressorTrainArgs(modelRef, frameRef, labelCol, obsCols, featureSubsetCategory = "all")
    assert(allFeatures.getMtries == 5)

    val sqrtFeatures = H2oRandomForestRegressorTrainArgs(modelRef, frameRef, labelCol, obsCols, featureSubsetCategory = "sqrt")
    assert(sqrtFeatures.getMtries == 3)

    val log2Features = H2oRandomForestRegressorTrainArgs(modelRef, frameRef, labelCol, obsCols, featureSubsetCategory = "log2")
    assert(log2Features.getMtries == 3)

    val thirdFeatures = H2oRandomForestRegressorTrainArgs(modelRef, frameRef, labelCol, obsCols, featureSubsetCategory = "onethird")
    assert(thirdFeatures.getMtries == 2)

    intercept[IllegalArgumentException] {
      val badFeatures = H2oRandomForestRegressorTrainArgs(modelRef, frameRef, labelCol, obsCols, featureSubsetCategory = "badcategory")
      badFeatures.getMtries
    }
  }

  "H2oRandomForestRegressorTrain" should "train random forest model" in {
    val modelRef = mock[ModelReference]
    val model = mock[Model]
    val frameRef = mock[FrameReference]

    val rows = sparkContext.parallelize(data)
    val frame = new FrameRdd(schema, rows)
    val trainArgs = H2oRandomForestRegressorTrainArgs(modelRef, frameRef, "label", List("obs1", "obs2"), 1, 4, featureSubsetCategory = "sqrt")
    val results = H2oRandomForestRegressorFunctions.train(frame, trainArgs, model)

    assert(results.varimp("obs1") > results.varimp("obs2"))
  }

  "H2oRandomForestRegressorTrain" should "predict random forest model" in {
    val h2oModel = H2oModelData(pojoKey, pojo, "label", List("obs1", "obs2"))
    val rows = sparkContext.parallelize(data)
    val frame = new FrameRdd(schema, rows)

    val predictFrame = H2oRandomForestRegressorFunctions.predict(frame, h2oModel, List("obs1", "obs2"))
    val predictedValues = predictFrame.mapRows(row => row.valuesAsDoubleArray(List("label", "predicted_value"))).collect()
    predictedValues.foreach(p => {
      val label = p(0)
      val prediction = p(1)
      assert(Math.abs(label - prediction) < epsilon)
    })
  }

  "H2oRandomForestRegressorTrain" should "test random forest model" in {
    val h2oModel = H2oModelData(pojoKey, pojo, "label", List("obs1", "obs2"))
    val rows = sparkContext.parallelize(data)
    val frame = new FrameRdd(schema, rows)

    val testMetrics = H2oRandomForestRegressorFunctions.getRegressionMetrics(frame, h2oModel, List("obs1", "obs2"), "label")

    assert(Math.abs(testMetrics.mae - 0) < epsilon)
    assert(Math.abs(testMetrics.mse - 0) < epsilon)
    assert(Math.abs(testMetrics.rmse - 0) < epsilon)
    assert(Math.abs(testMetrics.r2 - 1) < epsilon)
    assert(Math.abs(testMetrics.explainedVarianceScore - 1) < epsilon)
  }
}
