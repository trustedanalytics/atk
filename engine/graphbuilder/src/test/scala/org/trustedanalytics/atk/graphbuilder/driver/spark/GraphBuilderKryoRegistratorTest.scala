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

package org.trustedanalytics.atk.graphbuilder.driver.spark

import com.esotericsoftware.kryo.Kryo
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, GbIdToPhysicalId, Property }
import org.trustedanalytics.atk.graphbuilder.graph.GraphBuilderKryoRegistrator
import org.trustedanalytics.atk.graphbuilder.parser.ColumnDef
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ ConstantValue, EdgeRule, ParsedValue, Value }

class GraphBuilderKryoRegistratorTest extends WordSpec with Matchers with MockitoSugar {

  "GraphBuilderKryoRegistrator" should {

    "register expected classes" in {

      val kryo = mock[Kryo]

      // invoke method under test
      new GraphBuilderKryoRegistrator().registerClasses(kryo)

      // make sure all of the classes that will be serialized many times are included
      verify(kryo).register(classOf[GBEdge])
      verify(kryo).register(classOf[GBVertex])
      verify(kryo).register(classOf[Property])
      verify(kryo).register(classOf[Value])
      verify(kryo).register(classOf[ParsedValue])
      verify(kryo).register(classOf[ConstantValue])
      verify(kryo).register(classOf[GbIdToPhysicalId])

      // check some other ones too
      verify(kryo).register(classOf[EdgeRule])
      verify(kryo).register(classOf[ColumnDef])
    }

  }
}
