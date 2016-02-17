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

package org.apache.spark.ml

import org.apache.spark.ml.ScoringJsonReaderWriters._
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.DenseVector
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.MatcherUtils._
import spray.json._

class ScoringJsonReaderWritersTest extends FlatSpec with Matchers {

  "LinearRegressionModelFormat" should "serialize" in {
    val l = new LinearRegressionModel("Id", new DenseVector(Array(2.3, 3.4, 4.5)), 3.0)
    l.toJson.compactPrint should equal("{\"uid\":\"Id\",\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0}")
  }

  "LinearRegressionModelFormat" should "parse json" in {
    val string = "{\"uid\":\"Id\",\"weights\":{\"values\":[2.3,3.4,4.5]},\"intercept\":3.0}"
    val json = JsonParser(string).asJsObject
    val s = json.convertTo[LinearRegressionModel]

    s.weights.size should equal(3)
    s.intercept should equal(3.0)
    s.uid should equal("Id")
  }
}
