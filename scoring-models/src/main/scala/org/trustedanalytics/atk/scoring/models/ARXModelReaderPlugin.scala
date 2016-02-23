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

import org.apache.spark.mllib.ScoringJsonReaderWriters
import ScoringJsonReaderWriters.ARXDataFormat
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }
import com.cloudera.sparkts.ARXModel
import spray.json._

class ARXModelReaderPlugin() extends ModelLoader {

  private var myArxModel: ARXScoreModel = _

  override def load(bytes: Array[Byte]): Model = {
    val str = new String(bytes)
    val json: JsValue = str.parseJson
    val arxData = json.convertTo[ARXData]
    val arxModel = arxData.arxModel
    myArxModel = new ARXScoreModel(arxModel, arxData)
    myArxModel.asInstanceOf[Model]

  }
}