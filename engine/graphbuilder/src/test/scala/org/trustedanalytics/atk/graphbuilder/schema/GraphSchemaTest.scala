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

class GraphSchemaTest extends WordSpec with Matchers {

  "GraphSchema" should {

    "be able to provide properties by name" in {
      val propDef1 = new PropertyDef(PropertyType.Vertex, "one", classOf[String], true, true)
      val propDef2 = new PropertyDef(PropertyType.Vertex, "two", classOf[String], false, false)
      val propDef3 = new PropertyDef(PropertyType.Edge, "three", classOf[String], false, false)
      val propDef4 = new PropertyDef(PropertyType.Vertex, "one", classOf[String], false, false)

      // invoke method under test
      val schema = new GraphSchema(Nil, List(propDef1, propDef2, propDef3, propDef4))

      // validations
      schema.propertiesWithName("one").size shouldBe 2
      schema.propertiesWithName("two").size shouldBe 1
      schema.propertiesWithName("three").size shouldBe 1
      schema.propertiesWithName("three").head.propertyType shouldBe PropertyType.Edge
    }
  }
}
