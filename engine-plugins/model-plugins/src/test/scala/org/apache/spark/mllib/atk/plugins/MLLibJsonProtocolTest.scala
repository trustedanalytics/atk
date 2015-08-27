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

package org.apache.spark.mllib.atk.plugins

import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import org.apache.spark.mllib.tree.configuration.{ Algo, FeatureType }
import org.apache.spark.mllib.tree.configuration.FeatureType.FeatureType
import org.apache.spark.mllib.tree.model._
import org.trustedanalytics.atk.engine.model.plugins.classification.glm.LogisticRegressionData
import org.trustedanalytics.atk.engine.model.plugins.classification.SVMData
import org.trustedanalytics.atk.engine.model.plugins.clustering.KMeansData
import org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction.PrincipalComponentsData
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector, DenseMatrix }
import org.apache.spark.mllib.classification.{ LogisticRegressionModelWithFrequency, SVMModel }
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.scalatest.WordSpec
import MLLibJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.regression.LinearRegressionData

import spray.json._

class MLLibJsonProtocolTest extends WordSpec {
  "DenseVectorFormat" should {

    "be able to serialize" in {
      val dv = new DenseVector(Array(1.2, 3.4, 2.2))
      assert(dv.toJson.compactPrint == "{\"values\":[1.2,3.4,2.2]}")
    }

    "parse json" in {
      val string =
        """
          |{
          |   "values": [1.2,3.4,5.6,7.8]
          |
          |
          |}
        """.stripMargin
      val json = JsonParser(string).asJsObject
      val dv = json.convertTo[DenseVector]
      assert(dv.values.length == 4)
    }
  }

  "SparseVectorFormat" should {

    "be able to serialize" in {
      val sv = new SparseVector(2, Array(1, 2, 3), Array(1.5, 2.5, 3.5))
      assert(sv.toJson.compactPrint == "{\"size\":2,\"indices\":[1,2,3],\"values\":[1.5,2.5,3.5]}")
    }

    "parse json" in {
      val string =
        """
        |{
        |   "size": 3,
        |   "indices": [1,2,3,4],
        |   "values": [1.5,2.5,3.5,4.5]
        |
        |
        | }
      """.
          stripMargin
      val json = JsonParser(string).asJsObject
      val sv = json.convertTo[SparseVector]
      assert(sv.size == 3)
      assert(sv.indices.length == 4)
      assert(sv.values.length == 4)
    }
  }

  "KmeansModelFormat" should {

    "be able to serialize" in {
      val v = new KMeansModel(Array(new DenseVector(Array(1.2, 2.1)), new DenseVector(Array(3.4, 4.3))))
      assert(v.toJson.compactPrint == "{\"clusterCenters\":[{\"values\":[1.2,2.1]},{\"values\":[3.4,4.3]}]}")
    }

    "parse json" in {
      val string = "{\"clusterCenters\":[{\"values\":[1.2,2.1]},{\"values\":[3.4,4.3]}]}"
      val json = JsonParser(string).asJsObject
      val v = json.convertTo[KMeansModel]
      assert(v.clusterCenters.length == 2)
    }
  }

  "KMeansDataFormat" should {

    "be able to serialize" in {
      val d = new KMeansData(new KMeansModel(Array(new DenseVector(Array(1.2, 2.1)),
        new DenseVector(Array(3.4, 4.3)))), List("column1", "column2"), List(1.0, 2.0))
      assert(d.toJson.compactPrint == "{\"k_means_model\":{\"clusterCenters\":[{\"values\":[1.2,2.1]},{\"values\":[3.4,4.3]}]},\"observation_columns\":[\"column1\",\"column2\"],\"column_scalings\":[1.0,2.0]}")
    }

    "parse json" in {
      val string = "{\"k_means_model\":{\"clusterCenters\":[{\"values\":[1.2,2.1]},{\"values\":[3.4,4.3]}]},\"observation_columns\":[\"column1\",\"column2\"],\"column_scalings\":[1.0,2.0]}"
      val json = JsonParser(string).asJsObject
      val d = json.convertTo[KMeansData]
      assert(d.kMeansModel.clusterCenters.length == 2)
      assert(d.observationColumns.length == d.columnScalings.length)
      assert(d.observationColumns.length == 2)
    }
  }

  "LogisticRegressionDataFormat" should {

    "be able to serialize" in {
      val l = new LogisticRegressionData(new LogisticRegressionModelWithFrequency(new DenseVector(Array(1.3, 3.1)), 3.5), List("column1", "column2"))

      assert(l.toJson.compactPrint == "{\"log_reg_model\":{\"weights\":{\"values\":[1.3,3.1]},\"intercept\":3.5,\"numFeatures\":2,\"numClasses\":2},\"observation_columns\":[\"column1\",\"column2\"]}")

    }

    "parse json" in {
      val string = "{\"log_reg_model\":{\"weights\":{\"values\":[1.3,3.1,1.2]},\"intercept\":3.5, \"numFeatures\":3,\"numClasses\":2},\"observation_columns\":[\"column1\",\"column2\"]}"
      val json = JsonParser(string).asJsObject
      val l = json.convertTo[LogisticRegressionData]

      assert(l.logRegModel.weights.size == 3)
      assert(l.logRegModel.intercept == 3.5)
      assert(l.observationColumns.length == 2)
      assert(l.logRegModel.numFeatures == 3)
      assert(l.logRegModel.numClasses == 2)
    }
  }

  "SVMDataFormat" should {

    "be able to serialize" in {
      val s = new SVMData(new SVMModel(new DenseVector(Array(2.3, 3.4, 4.5)), 3.0), List("column1", "column2", "columns3", "column4"))
      assert(s.toJson.compactPrint == "{\"svm_model\":{\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}")
    }

    "parse json" in {
      val string = "{\"svm_model\":{\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}"
      val json = JsonParser(string).asJsObject
      val s = json.convertTo[SVMData]

      assert(s.svmModel.weights.size == 3)
      assert(s.svmModel.intercept == 3.0)
      assert(s.observationColumns.length == 4)
    }

  }

  "LinearRegressionDataFormat" should {

    "be able to serialize" in {
      val l = new LinearRegressionData(new LinearRegressionModel(new DenseVector(Array(1.3, 3.1)), 3.5), List("column1", "column2"))
      assert(l.toJson.compactPrint == "{\"lin_reg_model\":{\"weights\":{\"values\":[1.3,3.1]},\"intercept\":3.5},\"observation_columns\":[\"column1\",\"column2\"]}")
    }

    "parse json" in {
      val string = "{\"lin_reg_model\":{\"weights\":{\"values\":[1.3,3.1]},\"intercept\":3.5},\"observation_columns\":[\"column1\",\"column2\"]}"
      val json = JsonParser(string).asJsObject
      val l = json.convertTo[LinearRegressionData]

      assert(l.linRegModel.weights.size == 2)
      assert(l.linRegModel.intercept == 3.5)
      assert(l.observationColumns.length == 2)
    }
  }

  "DenseMatrixFormat" should {

    "be able to serialize" in {
      val dm = new DenseMatrix(2, 2, Array(1.0, 2.0, 3.0, 4.0), false)
      assert(dm.toJson.compactPrint == "{\"numRows\":2,\"numCols\":2,\"values\":[1.0,2.0,3.0,4.0],\"isTransposed\":false}")
    }

    "parse json" in {
      val string =
        """
        |{
        | "numRows": 2,
        | "numCols": 2,
        | "values": [1.0,2.0,3.0,4.0],
        | "isTransposed": false
        |}
      """.stripMargin
      val json = JsonParser(string).asJsObject
      val dm = json.convertTo[DenseMatrix]
      assert(dm.values.length == 4)
    }
  }

  "PrincipalComponentsModelFormat" should {

    "be able to serialize" in {
      val singularValuesVector = new DenseVector(Array(1.1, 2.2))
      val vFactorMatrix = new DenseMatrix(2, 2, Array(1.0, 2.0, 3.0, 4.0), false)
      val p = new PrincipalComponentsData(2, List("column1", "column2"), singularValuesVector, vFactorMatrix)
      assert(p.toJson.compactPrint == "{\"k\":2,\"observationColumns\":[\"column1\",\"column2\"]," +
        "\"singularValues\":{\"values\":[1.1,2.2]}," +
        "\"vFactor\":{\"numRows\":2,\"numCols\":2,\"values\":[1.0,2.0,3.0,4.0],\"isTransposed\":false}}")
    }

    "parse json" in {
      val string =
        """
          |{
          |"k":2,
          |"observationColumns": ["column1", "column2"],
          |"singularValues": {"values": [1.1,2.2]},
          |"vFactor": {"numRows":2, "numCols": 2, "values": [1.0,2.0,3.0,4.0], "isTransposed": false}
          |}
        """.stripMargin
      val json = JsonParser(string).asJsObject
      val p = json.convertTo[PrincipalComponentsData]

      assert(p.k == 2)
      assert(p.observationColumns.length == 2)
      assert(p.singularValues.size == 2)
      assert(p.vFactor.numRows == 2)
    }
  }

  "FeatureTypeModelFormat" should {

    "be able to serialize" in {
      val featureString = "Categorical"
      val featureType = FeatureType.withName(featureString)
      assert(featureType.toJson.compactPrint == "{\"featuretype\":\"Categorical\"}")
    }

    "parse json" in {
      val string = "{\"featuretype\":\"Categorical\"}"
      val json = JsonParser(string).asJsObject
      val f = json.convertTo[FeatureType]

      assert(f.toString == "Categorical")
      assert(f.id == 1)
    }
  }

  "AlgoModelFormat" should {

    "be able to seralize" in {
      val algoString = "Classification"
      val algo = Algo.withName(algoString)
      assert(algo.toJson.compactPrint == "{\"algo\":\"Classification\"}")
    }

    "parse json" in {
      val string = "{\"algo\":\"Classification\"}"
      val json = JsonParser(string).asJsObject
      val a = json.convertTo[Algo]

      assert(a.toString == "Classification")
      assert(a.id == 0)
    }
  }

  "SplitFormat" should {

    "be able to serialize" in {
      val feature = 2
      val threshold = 0.5
      val featureType = FeatureType.withName("Categorical")
      val categories = List(1.1, 2.2)
      val split = new Split(feature, threshold, featureType, categories)

      assert(split.toJson.compactPrint == "{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"},\"categories\":[1.1,2.2]}")
    }

    "parse json" in {
      val string = "{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"},\"categories\":[1.1,2.2]}"
      val json = JsonParser(string).asJsObject
      val split = json.convertTo[Split]

      assert(split.feature == 2)
      assert(split.threshold == 0.5)
      assert(split.featureType.toString == "Categorical")
      assert(split.categories.length == 2)
    }
  }

  "PredictFormat" should {

    "be able to serialize" in {
      val predict = 0.1
      val prob = 0.5
      val predictObject = new Predict(predict, prob)

      assert(predictObject.toJson.compactPrint == "{\"predict\":0.1,\"prob\":0.5}")
    }

    "parse json" in {
      val string = "{\"predict\":0.1,\"prob\":0.5}"
      val json = JsonParser(string).asJsObject
      val predictObject = json.convertTo[Predict]

      assert(predictObject.predict == 0.1)
      assert(predictObject.prob == 0.5)
    }
  }

  "InformationGainStatsFormat" should {

    "be able to serialize" in {
      val gain = 0.2
      val impurity = 0.3
      val leftImpurity = 0.4
      val rightImpurity = 0.5
      val leftPredict = new Predict(0.6, 0.7)
      val rightPredict = new Predict(0.8, 0.9)
      val i = new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, leftPredict, rightPredict)

      assert(i.toJson.compactPrint == "{\"gain\":0.2,\"impurity\":0.3,\"leftimpurity\":0.4,\"rightimpurity\":0.5," +
        "\"leftpredict\":{\"predict\":0.6,\"prob\":0.7},\"rightpredict\":{\"predict\":0.8,\"prob\":0.9}}")
    }

    "parse json" in {
      val string =
        """
          |{
          |"gain":0.2,
          |"impurity":0.3,
          |"leftimpurity":0.4,
          |"rightimpurity":0.5,
          |"leftpredict":{"predict":0.6,"prob":0.7},
          |"rightpredict":{"predict":0.8,"prob":0.9}
          |}
        """.stripMargin
      val json = JsonParser(string).asJsObject
      val i = json.convertTo[InformationGainStats]

      assert(i.gain == 0.2)
      assert(i.leftPredict.predict == 0.6)
    }
  }

  "NodeFormat" should {

    "be able to serialize" in {
      val id = 1
      val predict = new Predict(0.1, 0.2)
      val impurity = 0.2
      val isLeaf = false
      val split = new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))
      val leftNode = new Node(2, new Predict(0.3, 0.4), 0.5, true, Some(new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))), None, None, None)
      val node = new Node(id, predict, impurity, isLeaf, Some(split), Some(leftNode), None, None)

      assert(node.toJson.compactPrint == "{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2},\"impurity\":0.2,\"" +
        "isLeaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"leftNode\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5," +
        "\"isLeaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"leftNode\":null,\"rightNode\":null,\"stats\":null},\"rightNode\":null,\"stats\":null}")
    }

    "parse json" in {

      val string =
        """
          |{
          |"id":1,
          |"predict":{"predict":0.1,"prob":0.2},
          |"impurity":0.2,
          |"isLeaf":false,
          |"split":{"feature":2,"threshold":0.5,"featuretype":{"featuretype":"Categorical"},"categories":[1.1,2.2]},
          |"leftNode":null,
          |"rightNode":null,
          |"stats":null
          |}
      """.stripMargin
      val json = JsonParser(string).asJsObject
      val n = json.convertTo[Node]

      assert(n.id == 1)
    }
  }

  "DecisionTreeFormat" should {

    "be able to serialize" in {
      val split = new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))
      val leftNode = new Node(2, new Predict(0.3, 0.4), 0.5, true, Some(new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))), None, None, None)
      val topNode = new Node(1, new Predict(0.1, 0.2), 0.2, false, Some(split), Some(leftNode), None, None)
      val algoString = "Classification"
      val algo = Algo.withName(algoString)
      val decisionTree = new DecisionTreeModel(topNode, algo)

      assert(decisionTree.toJson.compactPrint == "{\"topnode\":{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2}," +
        "\"impurity\":0.2,\"isLeaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"leftNode\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4}," +
        "\"impurity\":0.5,\"isLeaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"leftNode\":null,\"rightNode\":null,\"stats\":null},\"rightNode\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}")
    }

    "parse json" in {
      val string = "{\"topnode\":{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2}," +
        "\"impurity\":0.2,\"isLeaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"leftNode\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4}," +
        "\"impurity\":0.5,\"isLeaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"leftNode\":null,\"rightNode\":null,\"stats\":null},\"rightNode\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}"

      val json = JsonParser(string).asJsObject
      val decisionTree = json.convertTo[DecisionTreeModel]

      assert(decisionTree.algo.toString == "Classification")
      assert(decisionTree.topNode.id == 1)
    }
  }

  "RandomForestModel" should {

    "be able to serialize" in {
      val split1 = new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))
      val leftNode1 = new Node(2, new Predict(0.3, 0.4), 0.5, true, Some(new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))), None, None, None)
      val topNode1 = new Node(1, new Predict(0.1, 0.2), 0.2, false, Some(split1), Some(leftNode1), None, None)
      val algoString1 = "Classification"
      val algo1 = Algo.withName(algoString1)
      val decisionTree1 = new DecisionTreeModel(topNode1, algo1)

      val split2 = new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))
      val leftNode2 = new Node(2, new Predict(0.3, 0.4), 0.5, true, Some(new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))), None, None, None)
      val topNode2 = new Node(2, new Predict(0.1, 0.2), 0.2, false, Some(split2), Some(leftNode2), None, None)
      val algoString2 = "Classification"
      val algo2 = Algo.withName(algoString2)
      val decisionTree2 = new DecisionTreeModel(topNode2, algo2)

      val trees = Array(decisionTree1, decisionTree2)
      val randomForest = new RandomForestModel(algo1, trees)

      assert(randomForest.toJson.compactPrint == "{\"algo\":{\"algo\":\"Classification\"},\"trees\":[{\"topnode\":{\"id\":1," +
        "\"predict\":{\"predict\":0.1,\"prob\":0.2},\"impurity\":0.2,\"isLeaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5," +
        "\"featuretype\":{\"featuretype\":\"Categorical\"},\"categories\":[1.1,2.2]},\"leftNode\":{\"id\":2,\"predict\":{\"predict\":0.3," +
        "\"prob\":0.4},\"impurity\":0.5,\"isLeaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"leftNode\":null,\"rightNode\":null,\"stats\":null},\"rightNode\":null,\"stats\":null}," +
        "\"algo\":{\"algo\":\"Classification\"}},{\"topnode\":{\"id\":2,\"predict\":{\"predict\":0.1,\"prob\":0.2},\"impurity\":0.2,\"isLeaf\":false," +
        "\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"},\"categories\":[1.1,2.2]},\"leftNode\":{\"id\":2," +
        "\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5,\"isLeaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5," +
        "\"featuretype\":{\"featuretype\":\"Categorical\"},\"categories\":[1.1,2.2]},\"leftNode\":null,\"rightNode\":null,\"stats\":null}," +
        "\"rightNode\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}]}")

    }

    "parse json" in {
      val string = "{\"algo\":{\"algo\":\"Classification\"},\"trees\":[{\"topnode\":{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2},\"impurity\":0.2," +
        "\"isLeaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"},\"categories\":[1.1,2.2]}," +
        "\"leftNode\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5,\"isLeaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5," +
        "\"featuretype\":{\"featuretype\":\"Categorical\"},\"categories\":[1.1,2.2]},\"leftNode\":null,\"rightNode\":null,\"stats\":null}," +
        "\"rightNode\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}},{\"topnode\":{\"id\":2,\"predict\":{\"predict\":0.1,\"prob\":0.2}," +
        "\"impurity\":0.2,\"isLeaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"leftNode\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5,\"isLeaf\":true," +
        "\"split\":{\"feature\":2,\"threshold\":0.5,\"featuretype\":{\"featuretype\":\"Categorical\"},\"categories\":[1.1,2.2]},\"leftNode\":null," +
        "\"rightNode\":null,\"stats\":null},\"rightNode\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}]}"

      val json = JsonParser(string).asJsObject
      val randomForest = json.convertTo[RandomForestModel]

      assert(randomForest.algo.toString == "Classification")
      assert(randomForest.trees(1).topNode.id == 2)

    }
  }

}
