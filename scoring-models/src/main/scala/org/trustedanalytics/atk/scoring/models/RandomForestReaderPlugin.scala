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

import java.io._

import org.trustedanalytics.atk.scoring.models.ScoringJsonReaderWriters._
import org.apache.spark.mllib.tree.configuration.{ FeatureType, Algo }
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector, Vector }
import spray.json._
import DefaultJsonProtocol._

class RandomForestReaderPlugin() extends ModelLoader {

  private var rfModel: RandomForestScoringModel = _

  override def load(bytes: Array[Byte]): Model = {
    try {
      val str = new String(bytes)
      println(str)
      val json: JsValue = str.parseJson
      val randomForestModel = json.convertTo[RandomForestModel]
      rfModel = new RandomForestScoringModel(randomForestModel)
      rfModel.asInstanceOf[Model]
    }
  }
}