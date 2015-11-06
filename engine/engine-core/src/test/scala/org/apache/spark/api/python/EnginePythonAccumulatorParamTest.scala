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

package org.apache.spark.api.python

import org.scalatest.WordSpec

class EnginePythonAccumulatorParamTest extends WordSpec {

  val accum = new EnginePythonAccumulatorParam()

  "EnginePythonAccumulatorParam" should {
    "have a zero value" in {
      assert(accum.zero(new java.util.ArrayList()).size() == 0)
    }

    "have a zero value when given a value" in {
      val list = new java.util.ArrayList[Array[Byte]]()
      list.add(Array[Byte](0))
      assert(accum.zero(list).size() == 0)
    }

    "should be able to add in place" in {
      val accum2 = new EnginePythonAccumulatorParam()

      val list1 = new java.util.ArrayList[Array[Byte]]()
      list1.add(Array[Byte](0))

      val list2 = new java.util.ArrayList[Array[Byte]]()
      list2.add(Array[Byte](0))

      assert(accum2.addInPlace(list1, list2).size() == 2)
    }
  }
}
