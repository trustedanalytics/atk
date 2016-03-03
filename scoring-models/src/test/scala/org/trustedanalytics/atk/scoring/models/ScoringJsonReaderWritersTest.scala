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
package org.trustedanalytics.atk.scoring.models

import org.apache.spark.mllib.ScoringJsonReaderWriters._
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import org.apache.spark.mllib.tree.configuration.FeatureType.FeatureType
import org.apache.spark.mllib.tree.configuration.{ Algo, FeatureType }
import org.apache.spark.mllib.tree.model._
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.MatcherUtils._
import spray.json._

class ScoringJsonReaderWritersTest extends FlatSpec with Matchers {
  val epsilon = 1e-6

  "LdaModelFormat" should "serialize LDA model to JSON" in {
    val topicWordMap = Map(
      "harry" -> Vector(0.9d, 0.1d),
      "economy" -> Vector(0.35d, 0.65d)
    )
    val ldaModel = LdaModel(2, topicWordMap, "doc", "word")
    val json = ldaModel.toJson
    json.compactPrint should equal("""{"num_topics":2,"topic_word_map":{"harry":[0.9,0.1],"economy":[0.35,0.65]},"document_column":"doc","word_column":"word"}""")
  }

  "LdaModelFormat" should "deserialize JSON to LDA model" in {
    val json = """{"num_topics":2,"topic_word_map":{"rain":[0.2,0.8],"weather":[0.35,0.65]}, "document_column" : "doc", "word_column": "word"}"""

    val ldaModel = JsonParser(json).asJsObject.convertTo[LdaModel]
    ldaModel.numTopics should equal(2)
    ldaModel.topicWordMap("rain").toArray should equalWithTolerance(Array(0.2d, 0.8d))
    ldaModel.topicWordMap("weather").toArray should equalWithTolerance(Array(0.35d, 0.65d))
    ldaModel.documentColumnName should equal("doc")
    ldaModel.wordColumnName should equal("word")
  }

  "LdaModelPredictReturnFormat" should "serialize LdaModelPredictReturn to JSON" in {
    val prediction = LdaModelPredictReturn(Vector(0.3d, 0.7d), 3, 18)
    val json = prediction.toJson
    print(json)
    json.compactPrint should equal("""{"topics_given_docs":[0.3,0.7],"new_words_count":3,"new_words_percentage":18.0}""")
  }

  "LdaModelPredictReturnFormat" should "deserialize JSON to LdaModelPredictReturn" in {
    val json = """{"topics_given_docs":[0.25,0.75],"new_words_count":7,"new_words_percentage":10.5}"""

    val prediction = JsonParser(json).asJsObject.convertTo[LdaModelPredictReturn]

    prediction.topicsGivenDoc.toArray should equalWithTolerance(Array(0.25d, 0.75d))
    prediction.newWordsCount should equal(7)
    prediction.newWordsPercentage should equal(10.5 +- epsilon)
  }

  "FeatureTypeModelFormat" should "be able to serialize" in {
    val featureString = "Categorical"
    val featureType = FeatureType.withName(featureString)
    featureType.toJson.compactPrint should equal("{\"feature_type\":\"Categorical\"}")
  }

  "FeatureTypeModelFormat" should "parse json" in {
    val string = "{\"feature_type\":\"Categorical\"}"
    val json = JsonParser(string).asJsObject
    val f = json.convertTo[FeatureType]

    f.toString should equal("Categorical")
    f.id should equal(1)
  }

  "AlgoModelFormat" should "be able to seralize" in {
    val algoString = "Classification"
    val algo = Algo.withName(algoString)
    algo.toJson.compactPrint should equal("{\"algo\":\"Classification\"}")
  }

  "AlgoModelFormat" should "parse json" in {
    val string = "{\"algo\":\"Classification\"}"
    val json = JsonParser(string).asJsObject
    val a = json.convertTo[Algo]

    a.toString should equal("Classification")
    a.id should equal(0)
  }

  "SplitFormat" should "be able to serialize" in {
    val feature = 2
    val threshold = 0.5
    val featureType = FeatureType.withName("Categorical")
    val categories = List(1.1, 2.2)
    val split = new Split(feature, threshold, featureType, categories)

    split.toJson.compactPrint should equal("{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]}")
  }

  "SplitFormat" should "parse json" in {
    val string = "{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]}"
    val json = JsonParser(string).asJsObject
    val split = json.convertTo[Split]

    split.feature should equal(2)
    split.threshold should equal(0.5)
    split.featureType.toString should equal("Categorical")
    split.categories.length should equal(2)
  }

  "PredictFormat" should "be able to serialize" in {
    val predict = 0.1
    val prob = 0.5
    val predictObject = new Predict(predict, prob)

    predictObject.toJson.compactPrint should equal("{\"predict\":0.1,\"prob\":0.5}")
  }

  "PredictFormat" should "parse json" in {
    val string = "{\"predict\":0.1,\"prob\":0.5}"
    val json = JsonParser(string).asJsObject
    val predictObject = json.convertTo[Predict]

    predictObject.predict should equal(0.1)
    predictObject.prob should equal(0.5)
  }

  "InformationGainStatsFormat" should "be able to serialize" in {
    val gain = 0.2
    val impurity = 0.3
    val leftImpurity = 0.4
    val rightImpurity = 0.5
    val leftPredict = new Predict(0.6, 0.7)
    val rightPredict = new Predict(0.8, 0.9)
    val i = new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, leftPredict, rightPredict)

    i.toJson.compactPrint should equal("{\"gain\":0.2,\"impurity\":0.3,\"left_impurity\":0.4,\"right_impurity\":0.5," +
      "\"left_predict\":{\"predict\":0.6,\"prob\":0.7},\"right_predict\":{\"predict\":0.8,\"prob\":0.9}}")
  }

  "InformationGainStatsFormat" should "parse json" in {
    val string =
      """
          |{
          |"gain":0.2,
          |"impurity":0.3,
          |"left_impurity":0.4,
          |"right_impurity":0.5,
          |"left_predict":{"predict":0.6,"prob":0.7},
          |"right_predict":{"predict":0.8,"prob":0.9}
          |}
        """.stripMargin
    val json = JsonParser(string).asJsObject
    val i = json.convertTo[InformationGainStats]

    i.gain should equal(0.2)
    i.leftPredict.predict should equal(0.6)
  }

  "NodeFormat" should "be able to serialize" in {
    val id = 1
    val predict = new Predict(0.1, 0.2)
    val impurity = 0.2
    val isLeaf = false
    val split = new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))
    val leftNode = new Node(2, new Predict(0.3, 0.4), 0.5, true, Some(new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))), None, None, None)
    val node = new Node(id, predict, impurity, isLeaf, Some(split), Some(leftNode), None, None)

    node.toJson.compactPrint should equal("{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2},\"impurity\":0.2,\"" +
      "is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
      "\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5," +
      "\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
      "\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null},\"right_node\":null,\"stats\":null}")
  }

  "NodeFormat" should "parse json" in {

    val string =
      """
          |{
          |"id":1,
          |"predict":{"predict":0.1,"prob":0.2},
          |"impurity":0.2,
          |"is_leaf":false,
          |"split":{"feature":2,"threshold":0.5,"feature_type":{"feature_type":"Categorical"},"categories":[1.1,2.2]},
          |"left_node":null,
          |"right_node":null,
          |"stats":null
          |}
        """.stripMargin
    val json = JsonParser(string).asJsObject
    val n = json.convertTo[Node]

    n.id should equal(1)
  }

  "DecisionTreeFormat" should "be able to serialize" in {
    val split = new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))
    val leftNode = new Node(2, new Predict(0.3, 0.4), 0.5, true, Some(new Split(2, 0.5, FeatureType.withName("Categorical"), List(1.1, 2.2))), None, None, None)
    val topNode = new Node(1, new Predict(0.1, 0.2), 0.2, false, Some(split), Some(leftNode), None, None)
    val algoString = "Classification"
    val algo = Algo.withName(algoString)
    val decisionTree = new DecisionTreeModel(topNode, algo)

    decisionTree.toJson.compactPrint should equal("{\"top_node\":{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2}," +
      "\"impurity\":0.2,\"is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
      "\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4}," +
      "\"impurity\":0.5,\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
      "\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null},\"right_node\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}")
  }

  "DecisionTreeFormat" should "parse json" in {
    val string = "{\"top_node\":{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2}," +
      "\"impurity\":0.2,\"is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
      "\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4}," +
      "\"impurity\":0.5,\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
      "\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null},\"right_node\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}"

    val json = JsonParser(string).asJsObject
    val decisionTree = json.convertTo[DecisionTreeModel]

    decisionTree.algo.toString should equal("Classification")
    decisionTree.topNode.id should equal(1)
  }

  "RandomForestModel" should "be able to serialize" in {
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

    randomForest.toJson.compactPrint should equal("{\"algo\":{\"algo\":\"Classification\"},\"trees\":[{\"top_node\":{\"id\":1," +
      "\"predict\":{\"predict\":0.1,\"prob\":0.2},\"impurity\":0.2,\"is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5," +
      "\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3," +
      "\"prob\":0.4},\"impurity\":0.5,\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
      "\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null},\"right_node\":null,\"stats\":null}," +
      "\"algo\":{\"algo\":\"Classification\"}},{\"top_node\":{\"id\":2,\"predict\":{\"predict\":0.1,\"prob\":0.2},\"impurity\":0.2,\"is_leaf\":false," +
      "\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2," +
      "\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5,\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5," +
      "\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null}," +
      "\"right_node\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}]}")
  }

  "RandomForestModel" should "parse json" in {
    val string = "{\"algo\":{\"algo\":\"Classification\"},\"trees\":[{\"top_node\":{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2},\"impurity\":0.2," +
      "\"is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]}," +
      "\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5,\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5," +
      "\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null}," +
      "\"right_node\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}},{\"top_node\":{\"id\":2,\"predict\":{\"predict\":0.1,\"prob\":0.2}," +
      "\"impurity\":0.2,\"is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
      "\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5,\"is_leaf\":true," +
      "\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]},\"left_node\":null," +
      "\"right_node\":null,\"stats\":null},\"right_node\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}]}"

    val json = JsonParser(string).asJsObject
    val randomForest = json.convertTo[RandomForestModel]

    randomForest.algo.toString should equal("Classification")
    randomForest.trees(1).topNode.id should equal(2)

  }

  "SVMModelFormat" should "serialize" in {
    val s = new SVMModel(new DenseVector(Array(2.3, 3.4, 4.5)), 3.0)
    s.toJson.compactPrint should equal("{\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0}")
  }

  "SVMModelFormat" should "parse json" in {
    val string = "{\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0}"
    val json = JsonParser(string).asJsObject
    val s = json.convertTo[SVMModel]

    s.weights.size should equal(3)
    s.intercept should equal(3.0)

  }

}

