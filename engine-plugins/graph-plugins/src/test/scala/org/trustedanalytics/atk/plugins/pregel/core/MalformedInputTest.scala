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
package org.trustedanalytics.atk.plugins.pregel.core

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * Tests that check that bad inputs and arguments will raise the appropriate exceptions.
 */

class MalformedInputTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait PregelTest {
    val args = TestInitializers.defaultPregelArgs()
  }

  "BP Runner" should "throw an illegal argument exception when vertices have different prior state space sizes " in new PregelTest {

    val vertexSet: Set[Long] = Set(1, 2)
    val pdfValues: Map[Long, Vector[Double]] = Map(
      1.toLong -> Vector(1.0d),
      2.toLong -> Vector(0.5d, 0.5d))

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Initializers.defaultEdgeSet()
    val gbVertexSet = vertexSet.map(x => GBVertex(x,
      Property(DefaultTestValues.vertexIdPropertyName, x),
      Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(DefaultTestValues.srcIdPropertyName, src),
            Property(DefaultTestValues.dstIdPropertyName, dst),
            DefaultTestValues.edgeLabel, Set.empty[Property]
          )
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(DefaultTestValues.vertexIdPropertyName, vid),
          Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(vid).get),
            Property(DefaultTestValues.outputPropertyName, pdfValues.get(vid).get))))
    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    // This on Spark, so the IllegalArgumentException bubbles up through a SparkException
    val exception = intercept[SparkException] {
      val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
        TestInitializers.defaultMsgSender,
        TestInitializers.defaultPregelVertexProgram,
        TestInitializers.defaultMsgSender
      )
    }

    exception.asInstanceOf[SparkException].getMessage should include("IllegalArgumentException")
    exception.asInstanceOf[SparkException].getMessage should include("Length of prior does not match state space size")
  }

  "BP Runner" should "throw a NotFoundException when the vertex does provide the request property" in new PregelTest {

    val vertexSet: Set[Long] = Set(1)
    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d))

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Initializers.defaultEdgeSet()
    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(DefaultTestValues.vertexIdPropertyName, x),
      Set(Property("wrongname", pdfValues.get(x).get))))
    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(DefaultTestValues.srcIdPropertyName, src),
            Property(DefaultTestValues.dstIdPropertyName, dst), DefaultTestValues.edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(DefaultTestValues.vertexIdPropertyName, vid),
          Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(vid).get),
            Property(DefaultTestValues.outputPropertyName, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    // This on Spark, so the IllegalArgumentException bubbles up through a SparkException
    val exception = intercept[Exception] {
      PregelAlgorithm.run(verticesIn, edgesIn, args)(
        TestInitializers.defaultMsgSender,
        TestInitializers.defaultPregelVertexProgram,
        TestInitializers.defaultMsgSender
      )
    }

    exception.asInstanceOf[Exception].getMessage should include("not be found")
    exception.asInstanceOf[Exception].getMessage should include(DefaultTestValues.inputPropertyName)
  }
}
