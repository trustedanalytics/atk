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
package org.trustedanalytics.atk.scoring.models

import hex.genmodel.GenModel
import org.apache.spark.h2o.H2oScoringModelData
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }
import spray.json._
import org.apache.spark.h2o.H2oScoringJsonReaderWriters._

class H2oRandomForestRegressorModelReaderPlugin() extends ModelLoader {

  private var genModel: GenModel = _

  override def load(bytes: Array[Byte]): Model = {
    val str = new String(bytes)
    val json: JsValue = str.parseJson
    val h2oModelData = json.convertTo[H2oScoringModelData]
    val classLoader = this.getClass.getClassLoader
    val rawModel = classLoader.loadClass(h2oModelData.modelName).newInstance()
    genModel = rawModel.asInstanceOf[GenModel]
    new H2oRandomForestRegressorScoreModel(h2oModelData, genModel)
  }
}
