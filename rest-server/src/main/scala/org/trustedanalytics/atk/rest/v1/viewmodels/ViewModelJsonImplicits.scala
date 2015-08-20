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

package org.trustedanalytics.atk.rest.v1.viewmodels

import org.trustedanalytics.atk.domain.command.CommandDefinition
import org.trustedanalytics.atk.domain.frame.FrameReference
import spray.httpx.SprayJsonSupport
import spray.json._
import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol

/**
 * Implicit Conversions for View/Models to JSON
 */
object ViewModelJsonImplicits extends AtkDefaultJsonProtocol with SprayJsonSupport {

  //this is needed for implicits
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._

  implicit val relLinkFormat = jsonFormat3(RelLink)
  implicit val getCommandsFormat = jsonFormat3(GetCommands)
  implicit val getCommandFormat = jsonFormat9(GetCommand)
  implicit val getDataFramesFormat = jsonFormat4(GetDataFrames)
  implicit val getDataFrameFormat = jsonFormat8(GetDataFrame)
  implicit val getGraphsFormat = jsonFormat4(GetGraphs)
  implicit val getGraphFormat = jsonFormat5(GetGraph)
  implicit val getModelFormat = jsonFormat5(GetModel)
  implicit val getModelsFormat = jsonFormat4(GetModels)
  implicit val getQueryPageFormat = jsonFormat4(GetQueryPage)
  implicit val getQueryPagesFormat = jsonFormat2(GetQueryPages)
  implicit val getQueriesFormat = jsonFormat3(GetQueries)
  implicit val getQueryFormat = jsonFormat8(GetQuery)
  implicit val jsonTransformFormat = jsonFormat2(JsonTransform)
}
