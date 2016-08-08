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

import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.scoring.interfaces.{ Field, Model, ModelMetaDataArgs }
import org.trustedanalytics.atk.scoring.{ DataOutputFormatJsonProtocol, ScoringServiceJsonProtocol }
import spray.json._

import scala.collection.immutable.Map

class ScoringServiceJsonProtocolTest extends WordSpec with Matchers {
  val model = new Model {
    override def input(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"))
    }

    override def modelMetadata(): ModelMetaDataArgs = {
      new ModelMetaDataArgs("Dummy Model", "Dummy Class", "Dummy Reader", Map("Created_On" -> "Jan 29th 2016"))
    }

    override def output(): Array[Field] = {
      Array(Field("col1", "Double"), Field("col2", "Double"), Field("col3", "double"), Field("score", "double"))
    }

    override def score(row: Array[Any]): Array[Any] = ???
  }

  import ScoringServiceJsonProtocol._
  val jsonFormat = new DataOutputFormatJsonProtocol(model)

  import jsonFormat.{ DataInputFormat, DataOutputFormat }
  //import jsonFormat.DataTypeJsonFormat

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
      val scores = Array(Map("col1" -> -1, "col2" -> -1, "col3" -> 1, "col4" -> 0.0))

      val output = DataOutputFormat.write(scores.asInstanceOf[Array[Map[String, Any]]])
      assert(output != null)
      assert(output.compactPrint == "{\"data\":[{\"col1\":-1,\"col2\":-1,\"col3\":1,\"col4\":0.0}]}")
    }

  }

  "DataTypeJsonFormat" should {
    "construct a Json Object" in {
      val scores = Array("test_string", 1.0d, Map("int_key" -> 1, "list_key" -> List(2.0, 3.0)))

      val output = DataTypeJsonFormat.write(scores)
      assert(output != null)
      assert(output.compactPrint == """["test_string",1.0,{"int_key":1,"list_key":[2.0,3.0]}]""")
    }

    "construct a Json Object from an input map" in {
      val scores = Map("int_key" -> 1, "string_key" -> "test", "int_list_key" -> List(2.0, 3.0),
        "string_list_key" -> List("a", "b"), "vector_key" -> Vector(4d, 5d)
      )

      val output: JsObject = DataTypeJsonFormat.write(scores).asJsObject
      assert(output != null)
      assert(output.fields.size == 5)
      assert(output.fields("int_key").compactPrint == """1""")
      assert(output.fields("int_list_key").compactPrint == """[2.0,3.0]""")
      assert(output.fields("string_list_key").compactPrint == """["a","b"]""")
      assert(output.fields("vector_key").compactPrint == """[4.0,5.0]""")
      assert(output.fields("string_key").compactPrint == """"test"""")
    }

    "construct a Json Object from an input list" in {
      val scores = List(1, "test_string", Array(2.0, 3.0), Map("list_key" -> List(2.0, 3.0)))

      val output = DataTypeJsonFormat.write(scores)
      assert(output != null)
      assert(output.compactPrint == """[1,"test_string",[2.0,3.0],{"list_key":[2.0,3.0]}]""")
    }

    "parse JSON input" in {
      val string = """["test_string",1, {"int_key":1,"list_key":[2,3]}]"""
      val json = JsonParser(string)
      val input = DataTypeJsonFormat.read(json).asInstanceOf[List[Any]]
      assert(input != null)
      assert(input(0) == "test_string")
      assert(input(1) == 1)
      assert(input(2).isInstanceOf[Map[String, Any]])

      val map = input(2).asInstanceOf[Map[String, Any]]
      assert(map("int_key") == 1)
      assert(map("list_key").isInstanceOf[List[Int]])

      val list = map("list_key").asInstanceOf[List[Int]]
      assert(list.length == 2)
      assert(list(0) == 2)
      assert(list(1) == 3)
    }
  }
}

