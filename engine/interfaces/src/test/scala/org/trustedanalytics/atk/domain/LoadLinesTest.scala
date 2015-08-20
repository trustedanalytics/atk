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

package org.trustedanalytics.atk.domain

import org.trustedanalytics.atk.domain.frame.load._
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import spray.json._
import DomainJsonProtocol._

class LoadLinesTest extends FlatSpec with Matchers {

  "Load" should "parse a Load object with for a file source" in {
    val string =
      """
          |{
          |    "destination": "ta://frame/5",
          |    "source": {
          |      "source_type": "file",
          |      "uri": "m1demo/domains.json",
          |      "parser": {
          |        "name": "builtin/line/separator",
          |        "arguments": {
          |          "separator": "`",
          |          "skip_rows": 0,
          |          "schema": {
          |            "columns": [["json", "str"]]
          |          }
          |        }
          |      }
          |    }
          |}
          |
        """.stripMargin
    val myJson = JsonParser(string).asJsObject
    val myLoadLines = myJson.convertTo[LoadFrameArgs]

    myLoadLines.source.sourceType should be("file")
    myLoadLines.source.parser should not be None
    val parser = myLoadLines.source.parser.get

    parser.name should be("builtin/line/separator")
    parser.arguments should be(LineParserArguments('`', new SchemaArgs(List(("json", DataTypes.string))), Some(0)))
  }

  "Load" should "parse a Load object with for a frame source" in {
    val string =
      """
          |{
          |    "destination": "ta://frame/5",
          |    "source": {
          |      "source_type": "frame",
          |      "uri": "http://localhost:9099/v1/frames/5"
          |    }
          |}
          |
        """.stripMargin
    val myJson = JsonParser(string).asJsObject
    val myLoadLines = myJson.convertTo[LoadFrameArgs]

    myLoadLines.source.uri should be("http://localhost:9099/v1/frames/5")
    myLoadLines.source.sourceType should be("frame")
    myLoadLines.source.parser should be(None)
  }

  "LoadSource" should "be parsed from a JSON that does include a parser" in {
    val json =
      """
          |{
          |  "source_type": "file",
          |  "uri": "m1demo/domains.json",
          |  "parser": {
          |    "name": "builtin/line/separator",
          |    "arguments": {
          |      "separator": "`",
          |      "skip_rows": 0,
          |      "schema": {
          |        "columns": [
          |          ["json", "str"]
          |        ],
          |         "frame_type": "VertexFrame",
          |         "label": "test label"
          |      }
          |    }
          |  }
          |}
        """.stripMargin
    val myJson = JsonParser(json).asJsObject
    val mySource = myJson.convertTo[LoadSource]
    mySource.sourceType should be("file")
    mySource.uri should be("m1demo/domains.json")

    mySource.parser should not be None
  }

}
