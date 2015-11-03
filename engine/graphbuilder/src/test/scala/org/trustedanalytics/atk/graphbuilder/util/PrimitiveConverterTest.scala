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


package org.trustedanalytics.atk.graphbuilder.util

import org.trustedanalytics.atk.graphbuilder.elements.GBVertex
import org.scalatest.FlatSpec

class PrimitiveConverterTest extends FlatSpec {

  "PrimitiveConverter" should "be able to convert ints" in {
    PrimitiveConverter.primitivesToObjects(classOf[Int]) == classOf[java.lang.Integer]
  }

  it should "be able to convert longs" in {
    PrimitiveConverter.primitivesToObjects(classOf[Long]) == classOf[java.lang.Long]
  }

  it should "be able to convert booleans" in {
    PrimitiveConverter.primitivesToObjects(classOf[Boolean]) == classOf[java.lang.Boolean]
  }

  it should "be able to convert chars" in {
    PrimitiveConverter.primitivesToObjects(classOf[Char]) == classOf[java.lang.Character]
  }

  it should "be able to convert floats" in {
    PrimitiveConverter.primitivesToObjects(classOf[Float]) == classOf[java.lang.Float]
  }

  it should "be able to convert doubles" in {
    PrimitiveConverter.primitivesToObjects(classOf[Double]) == classOf[java.lang.Double]
  }

  it should "be able to convert bytes" in {
    PrimitiveConverter.primitivesToObjects(classOf[Byte]) == classOf[java.lang.Byte]
  }

  it should "be able to convert shorts" in {
    PrimitiveConverter.primitivesToObjects(classOf[Short]) == classOf[java.lang.Short]
  }

  it should "NOT convert non-primitive types like Vertex" in {
    PrimitiveConverter.primitivesToObjects(classOf[GBVertex]) == classOf[GBVertex]
  }

  it should "NOT convert non-primitive types like List" in {
    PrimitiveConverter.primitivesToObjects(classOf[List[String]]) == classOf[List[String]]
  }
}
