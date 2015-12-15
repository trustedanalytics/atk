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

package org.trustedanalytics.atk.plugins.pregel.lbp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.trustedanalytics.atk.plugins.pregel.core.{ PregelAlgorithm, PregelArgs }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * Tests that check that bad inputs and arguments will raise the appropriate exceptions.
 */

class MalformedInputTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait BPTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold: Double = 0.000000001d

    val args = PregelArgs(
      priorProperty = inputPropertyName,
      edgeWeightProperty = StringUtils.EMPTY,
      maxIterations = 10,
      stringOutput = false,
      convergenceThreshold = 0d,
      posteriorProperty = propertyForLBPOutput,
      stateSpaceSize = 2)

  }

  "BP Runner" should "throw an illegal argument exception when vertices have different prior state space sizes " in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)
    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d), 2.toLong -> Vector(0.5d, 0.5d))

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set()
    val gbVertexSet = vertexSet.map(x => GBVertex(x,
      Property(vertexIdPropertyName, x),
      Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(srcIdPropertyName, src),
            Property(dstIdPropertyName, dst),
            edgeLabel, Set.empty[Property]
          )
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))
    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    // This on Spark, so the IllegalArgumentException bubbles up through a SparkException
    val exception = intercept[SparkException] {
      val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
        LoopyBeliefPropagationMessage.msgSender,
        LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
        LoopyBeliefPropagationMessage.msgSender
      )
    }

    exception.asInstanceOf[SparkException].getMessage should include("IllegalArgumentException")
    exception.asInstanceOf[SparkException].getMessage should include("Length of prior does not match state space size")
  }

  "BP Runner" should "throw a NotFoundException when the vertex does provide the request property" in new BPTest {

    val vertexSet: Set[Long] = Set(1)
    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d))

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set()
    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property("hahawrongname", pdfValues.get(x).get))))
    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    // This on Spark, so the IllegalArgumentException bubbles up through a SparkException
    val exception = intercept[Exception] {
      PregelAlgorithm.run(verticesIn, edgesIn, args)(
        LoopyBeliefPropagationMessage.msgSender,
        LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
        LoopyBeliefPropagationMessage.msgSender
      )
    }

    exception.asInstanceOf[Exception].getMessage should include("not be found")
    exception.asInstanceOf[Exception].getMessage should include(inputPropertyName)
  }
}
