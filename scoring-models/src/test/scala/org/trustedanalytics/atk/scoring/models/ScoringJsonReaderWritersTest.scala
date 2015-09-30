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
}
