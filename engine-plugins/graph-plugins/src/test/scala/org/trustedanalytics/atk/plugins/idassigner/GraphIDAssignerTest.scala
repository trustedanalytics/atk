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
package org.trustedanalytics.atk.plugins.idassigner

import org.scalatest.{ FlatSpec, Matchers }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class GraphIDAssignerTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait GraphIDAssignerTest {
    val one: String = "1"
    val two: String = "2"
    val three: String = "3"
    val four: String = "4"
    val five: String = "5"
    val six: String = "6"
    val seven: String = "7"

    val vertexList: List[String] = List(one, two, three, four, five, six, seven)

    // to save ourselves some pain we enter the directed edge list and then make it bidirectional with a flatmap

    val edgeList: List[(String, String)] = List((two, six), (two, four), (four, six),
      (three, five), (three, seven), (five, seven)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val idAssigner = new GraphIDAssigner[String]()
  }

  "ID assigner" should
    "produce distinct IDs" in new GraphIDAssignerTest {

      val out = idAssigner.run(sparkContext.parallelize(vertexList, 3), sparkContext.parallelize(edgeList, 3))

      out.vertices.distinct().count() shouldEqual out.vertices.count()
    }

  "ID assigner" should
    "produce one  ID for each incoming vertex" in new GraphIDAssignerTest {

      val out = idAssigner.run(sparkContext.parallelize(vertexList, 3), sparkContext.parallelize(edgeList, 3))

      out.vertices.count() shouldEqual vertexList.size
    }

  "ID assigner" should
    "produce the same number of edges in the renamed graph as in the input graph" in new GraphIDAssignerTest {

      val out = idAssigner.run(sparkContext.parallelize(vertexList, 3), sparkContext.parallelize(edgeList, 3))

      out.edges.count() shouldEqual edgeList.size
    }

  "ID assigner" should
    "have every edge in the renamed graph correspond to an edge in the old graph under the provided inverse renaming " in
    new GraphIDAssignerTest {

      val out = idAssigner.run(sparkContext.parallelize(vertexList, 3), sparkContext.parallelize(edgeList, 3))

      val renamedEdges: Array[(Long, Long)] = out.edges.collect()

      def renamed(srcNewId: Long, dstNewId: Long) = {
        (out.newIdsToOld.lookup(srcNewId), out.newIdsToOld.lookup(dstNewId))
      }

      renamedEdges.forall({ case (srcNewId, dstNewId) => edgeList.contains(renamed(srcNewId, dstNewId)) })
    }

  /**
   * We do not need to check that every edge in the base graph can be found in the renamed graph because we have checked
   * that the count of edges is the same in both graphs,  we know that every renamed edge is in the base graph
   * (after we undo the renaming), and we know that the renaming is an injection from the vertex set.
   */
}
