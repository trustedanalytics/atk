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
package org.trustedanalytics.atk.scoring.models

import org.scalatest.{FlatSpec, Matchers}
import org.trustedanalytics.atk.scoring.models.ScoringJsonReaderWriters._
import org.trustedanalytics.atk.testutils.MatcherUtils._
import spray.json._
import ScoringJsonReaderWriters._

class ScoringJsonReaderWritersTest extends FlatSpec with Matchers {
  val epsilon = 1e-6

  "LdaModelFormat" should "serialize LDA model to JSON" in {
    val topicWordMap = Map(
      "harry" -> Vector(0.9d, 0.1d),
      "economy" -> Vector(0.35d, 0.65d)
    )
    val ldaModel = LdaModel(2, topicWordMap)
    val json = ldaModel.toJson
    json.compactPrint should equal("""{"num_topics":2,"topic_word_map":{"harry":[0.9,0.1],"economy":[0.35,0.65]}}""")
  }

  "LdaModelFormat" should "deserialize JSON to LDA model" in {
    val json = """{"num_topics":2,"topic_word_map":{"rain":[0.2,0.8],"weather":[0.35,0.65]}}"""

    val ldaModel = JsonParser(json).asJsObject.convertTo[LdaModel]
    ldaModel.numTopics should equal(2)
    ldaModel.topicWordMap("rain").toArray should equalWithTolerance(Array(0.2d, 0.8d))
    ldaModel.topicWordMap("weather").toArray should equalWithTolerance(Array(0.35d, 0.65d))
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

  "FeatureTypeModelFormat" should {

    "be able to serialize" in {
      val featureString = "Categorical"
      val featureType = FeatureType.withName(featureString)
      assert(featureType.toJson.compactPrint == "{\"feature_type\":\"Categorical\"}")
    }

    "parse json" in {
      val string = "{\"feature_type\":\"Categorical\"}"
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

      assert(split.toJson.compactPrint == "{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]}")
    }

    "parse json" in {
      val string = "{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"},\"categories\":[1.1,2.2]}"
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

      assert(i.toJson.compactPrint == "{\"gain\":0.2,\"impurity\":0.3,\"left_impurity\":0.4,\"right_impurity\":0.5," +
        "\"left_predict\":{\"predict\":0.6,\"prob\":0.7},\"right_predict\":{\"predict\":0.8,\"prob\":0.9}}")
    }

    "parse json" in {
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
        "is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4},\"impurity\":0.5," +
        "\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null},\"right_node\":null,\"stats\":null}")
    }

    "parse json" in {

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

      assert(decisionTree.toJson.compactPrint == "{\"top_node\":{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2}," +
        "\"impurity\":0.2,\"is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4}," +
        "\"impurity\":0.5,\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null},\"right_node\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}")
    }

    "parse json" in {
      val string = "{\"top_node\":{\"id\":1,\"predict\":{\"predict\":0.1,\"prob\":0.2}," +
        "\"impurity\":0.2,\"is_leaf\":false,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"left_node\":{\"id\":2,\"predict\":{\"predict\":0.3,\"prob\":0.4}," +
        "\"impurity\":0.5,\"is_leaf\":true,\"split\":{\"feature\":2,\"threshold\":0.5,\"feature_type\":{\"feature_type\":\"Categorical\"}," +
        "\"categories\":[1.1,2.2]},\"left_node\":null,\"right_node\":null,\"stats\":null},\"right_node\":null,\"stats\":null},\"algo\":{\"algo\":\"Classification\"}}"

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

      assert(randomForest.toJson.compactPrint == "{\"algo\":{\"algo\":\"Classification\"},\"trees\":[{\"top_node\":{\"id\":1," +
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

    "parse json" in {
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

      assert(randomForest.algo.toString == "Classification")
      assert(randomForest.trees(1).topNode.id == 2)

    }

  }
