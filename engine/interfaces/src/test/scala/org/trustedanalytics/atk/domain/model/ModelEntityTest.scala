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
package org.trustedanalytics.atk.domain.model

import org.joda.time.DateTime
import org.scalatest.WordSpec

class ModelEntityTest extends WordSpec {

  val model = new ModelEntity(1, Some("name"), "model:subtype", None, 1, None, new DateTime(), new DateTime())

  "Model" should {

    "require an id greater than zero" in {
      intercept[IllegalArgumentException] { model.copy(id = -1) }
    }

    "require a name" in {
      intercept[IllegalArgumentException] { model.copy(name = null) }
    }

    "require a non-empty name" in {
      intercept[IllegalArgumentException] { model.copy(name = Some("")) }
    }

    "require a modelType" in {
      intercept[IllegalArgumentException] { model.copy(modelType = null) }
    }

    //"require a modelType that starts with 'model:'" in {
    //  intercept[IllegalArgumentException] { model.copy(modelType = "frame") }
    //}

  }

}
