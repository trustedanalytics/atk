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

package org.trustedanalytics.atk.graphbuilder.schema

import org.scalatest.{ Matchers, WordSpec }

class PropertyDefTest extends WordSpec with Matchers {

  "PropertyDef" should {

    "require a name" in {
      an[IllegalArgumentException] should be thrownBy new PropertyDef(PropertyType.Vertex, null, null, false, false)
    }
  }
}
