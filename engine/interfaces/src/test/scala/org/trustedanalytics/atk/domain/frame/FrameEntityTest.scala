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
package org.trustedanalytics.atk.domain.frame

import org.scalatest.WordSpec
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Schema }
import org.joda.time.DateTime

class FrameEntityTest extends WordSpec {

  val frame = new FrameEntity(id = 1,
    name = Some("name"),
    status = 1,
    createdOn = new DateTime,
    modifiedOn = new DateTime)

  "DataFrame" should {

    "require an id greater than zero" in {
      intercept[IllegalArgumentException] {
        frame.copy(id = -1)
      }
    }

    "require a name" in {
      intercept[IllegalArgumentException] {
        frame.copy(name = null)
      }
    }

    "require a non-empty name" in {
      intercept[IllegalArgumentException] {
        frame.copy(name = Some(""))
      }
    }

    "require a parent greater than zero" in {
      intercept[IllegalArgumentException] {
        frame.copy(parent = Some(-1))
      }
    }
  }
}
