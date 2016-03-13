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
package org.trustedanalytics.atk.plugins.communitydetection.kclique

import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes.{ Clique, CliqueExtension, Edge }
import org.scalatest.{ Matchers, FlatSpec }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes.Clique
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class CliqueExtensionEnumeratorTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait KCliqueEnumTest {

    val vertexWithAdjacencyList: List[(Long, Array[Long])] = List((1, Array(2, 3, 4)), (2, Array(3, 4)), (3, Array(4, 5))).map(
      { case (v, nbrs) => (v.toLong, nbrs.map(_.toLong)) })

    val edgeList: List[(Long, Long)] = List((1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4), (3, 5)).map({ case (x, y) => (x.toLong, y.toLong) })

    val fourCliques = List((Array(1, 2, 3), Array(4))).map({ case (cliques, extenders) => (cliques.map(_.toLong).toSet, extenders.map(_.toLong).toSet) })

  }

  "K-Clique enumeration" should
    "create all set of k-cliques" in new KCliqueEnumTest {

      val rddOfEdgeList: RDD[Edge] = sparkContext.parallelize(edgeList).map(keyval => Edge(keyval._1, keyval._2))
      val rddOfFourCliques = sparkContext.parallelize(fourCliques).map({ case (x, y) => CliqueExtension(Clique(x), y, neighborsHigh = true) })

      val enumeratedFourCliques = CliqueExtensionEnumerator.run(rddOfEdgeList, 4)

      enumeratedFourCliques.collect().toSet shouldEqual rddOfFourCliques.collect().toSet

    }

}
