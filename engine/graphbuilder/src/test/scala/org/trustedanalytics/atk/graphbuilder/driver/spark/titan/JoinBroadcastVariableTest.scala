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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan

import org.trustedanalytics.atk.graphbuilder.elements.Property
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class JoinBroadcastVariableTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {
  val personIds: List[(Property, AnyRef)] = List(
    (Property("name", "alice"), new java.lang.Long(1)),
    (Property("name", "bob"), new java.lang.Long(2)),
    (Property("name", "cathy"), new java.lang.Long(3)),
    (Property("name", "david"), new java.lang.Long(4)),
    (Property("name", "eva"), new java.lang.Long(5))
  )

  "JoinBroadcastVariable" should "create a single broadcast variable when RDD size is less than 2GB" in {
    val personRdd = sparkContext.parallelize(personIds)

    val broadcastVariable = JoinBroadcastVariable(personRdd)

    broadcastVariable.broadcastMap.value.size should equal(5)
    personIds.map { case (property, id) => broadcastVariable.get(property) should equal(Some(id)) }
    broadcastVariable.get(Property("not defined", new java.lang.Long(1))).isDefined should equal(false)
  }
  "JoinBroadcastVariable" should "create an empty broadcast variable" in {
    val emptyList = sparkContext.parallelize(List.empty[(Property, AnyRef)])

    val broadcastVariable = JoinBroadcastVariable(emptyList)

    broadcastVariable.broadcastMap.value.isEmpty should equal(true)
  }
  "JoinBroadcastVariable" should "throw an IllegalArgumentException if join parameter is null" in {
    intercept[IllegalArgumentException] {
      JoinBroadcastVariable(null)
    }
  }
}
