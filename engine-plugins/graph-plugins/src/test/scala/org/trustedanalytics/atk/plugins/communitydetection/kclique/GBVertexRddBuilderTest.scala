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

package org.trustedanalytics.atk.plugins.communitydetection.kclique

import org.trustedanalytics.atk.graphbuilder.driver.spark.elements.{ Property, GBVertex }
import org.trustedanalytics.atk.plugins.communitydetection.ScalaToJavaCollectionConverter
import org.scalatest.{ Matchers, FlatSpec }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class GBVertexRddBuilderTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait GBVertexSetTest {

    val communityPropertyDefaultLabel: String = "Community"

    val gbVerticesList: List[GBVertex] = List(
      new GBVertex(java.lang.Long.valueOf(10001), new Property("physicalId", 10001), Set(new Property("source", 101))),
      new GBVertex(java.lang.Long.valueOf(10002), new Property("physicalId", 10002), Set(new Property("source", 102))),
      new GBVertex(java.lang.Long.valueOf(10003), new Property("physicalId", 10003), Set(new Property("source", 103))),
      new GBVertex(java.lang.Long.valueOf(10004), new Property("physicalId", 10004), Set(new Property("source", 104))))

    val vertexCommunitySet: List[(Long, Set[Long])] = List(
      (10001, Array(1)),
      (10003, Array(1, 2)),
      (10004, Array(1))
    ).map({ case (vertex, communityArray) => (vertex.toLong, communityArray.map(_.toLong).toSet) })

    val emptySet: Set[Long] = Set()
    val newGBVertexList = List(
      new GBVertex(java.lang.Long.valueOf(10001), new Property("physicalId", 10001),
        Set(new Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(Set(1.toLong))))),
      new GBVertex(java.lang.Long.valueOf(10002), new Property("physicalId", 10002),
        Set(new Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(emptySet)))),
      new GBVertex(java.lang.Long.valueOf(10003), new Property("physicalId", 10003),
        Set(new Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(Set(1.toLong, 2.toLong))))),
      new GBVertex(java.lang.Long.valueOf(10004), new Property("physicalId", 10004),
        Set(new Property(communityPropertyDefaultLabel, ScalaToJavaCollectionConverter.convertSet(Set(1.toLong))))))

  }

  "Number of vertices coming out" should
    "be same with original input graph" in new GBVertexSetTest {

      val rddOfGbVerticesList: RDD[GBVertex] = sparkContext.parallelize(gbVerticesList)
      val rddOfVertexCommunitySet: RDD[(Long, Set[Long])] = sparkContext.parallelize(vertexCommunitySet)

      val gbVertexSetter: GBVertexRddBuilder = new GBVertexRddBuilder(rddOfGbVerticesList, rddOfVertexCommunitySet)
      val newGBVerticesAsGBVertexSetterOutput: RDD[GBVertex] = gbVertexSetter.setVertex(communityPropertyDefaultLabel)

      newGBVerticesAsGBVertexSetterOutput.count() shouldEqual rddOfGbVerticesList.count()

    }

}
