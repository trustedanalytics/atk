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

package org.trustedanalytics.atk.domain

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.scoring.ScoringServiceJsonProtocol
import org.scalatest.{ Matchers, WordSpec }
import spray.json._
import org.trustedanalytics.atk.scoring.interfaces.{ Field, Model }

class ScoringServiceJsonProtocolTest extends WordSpec with Matchers {
  val model = new Model {
    override def input(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"))
    }

    override def modelMetadata(): Map[String, String] = {
      Map("Model Type" -> "Dummy Model", "Class Name" -> "Model Class", "Model Reader" -> "Model Reader", "Created On" -> "Jan 29th 2016")
    }

    override def output(): Array[Field] = ???

    override def score(row: Array[Any]): Array[Any] = ???
  }

  val jsonFormat = new ScoringServiceJsonProtocol(model)

  import jsonFormat.DataInputFormat
  import jsonFormat.DataOutputFormat

  "DataInputFormat" should {
    "parse JSON input" in {
      val string =
        """
          |{
          |   "records": [
          |           {"col1": -1, "col2": -1, "col3": -1}, {"col1": 0, "col2": -2, "col3": 1}
          |    ]
          |}
        """.stripMargin
      val json = JsonParser(string).asJsObject
      val input = DataInputFormat.read(json)
      assert(input != null)
      assert(input.length == 2)
      assert(input.head.length == 3)
      assert(input(1).length == 3)
      assert(input.head(0) == -1.0)
    }
  }

  "DataOutputFormat" should {
    "construct a Json Object" in {
      var scores = Array("-1", "-1", "-1", "0.0")

      val output = DataOutputFormat.write(scores.asInstanceOf[Array[Any]])
      assert(output != null)
      assert(output.compactPrint == "{\"Model Details\":[{\"Model Type\":\"Dummy Model\"},{\"Class Name\":\"Model Class\"},{\"Model Reader\":\"Model Reader\"},{\"Created On\":\"Jan 29th 2016\"}],\"Input\":[{\"name\":\"col1\",\"value\":\"Double\"},{\"name\":\"col2\",\"value\":\"Double\"},{\"name\":\"col3\",\"value\":\"double\"}],\"output\":[\"-1\",\"-1\",\"-1\",\"0.0\"]}")
    }

  }
}

