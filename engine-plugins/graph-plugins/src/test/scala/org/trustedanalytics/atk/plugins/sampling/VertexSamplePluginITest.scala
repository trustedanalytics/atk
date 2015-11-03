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

package org.trustedanalytics.atk.plugins.sampling

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.testutils.TestingTitan
import org.scalatest.{ BeforeAndAfter, Matchers }
import org.trustedanalytics.atk.testutils.{ TestingTitan, TestingSparkContextWordSpec }

import scala.collection.JavaConversions._

/**
 * Integration testing for uniform vertex sampling
 */
class VertexSamplePluginITest extends TestingSparkContextWordSpec with Matchers {

  // generate sample data
  val gbIds = Map((1, new Property("gbId", 1)),
    (2, new Property("gbId", 2)),
    (3, new Property("gbId", 3)),
    (4, new Property("gbId", 4)),
    (5, new Property("gbId", 5)),
    (6, new Property("gbId", 6)),
    (7, new Property("gbId", 7)),
    (8, new Property("gbId", 8)))

  val inputVertexList = Seq(GBVertex(gbIds(1), gbIds(1), Set(new Property("location", "Oregon"))),
    GBVertex(gbIds(2), gbIds(2), Set(new Property("location", "Oregon"))),
    GBVertex(gbIds(3), gbIds(3), Set(new Property("location", "Oregon"))),
    GBVertex(gbIds(4), gbIds(4), Set(new Property("location", "Oregon"))),
    GBVertex(gbIds(5), gbIds(5), Set(new Property("location", "Oregon"))),
    GBVertex(gbIds(6), gbIds(6), Set(new Property("location", "Oregon"))),
    GBVertex(gbIds(7), gbIds(7), Set(new Property("location", "Oregon"))),
    GBVertex(gbIds(8), gbIds(8), Set(new Property("location", "Oregon"))))

  val inputVertexListWeighted = Seq((0.5, GBVertex(gbIds(1), gbIds(1), Set(new Property("location", "Oregon")))),
    (0.1, GBVertex(gbIds(2), gbIds(2), Set(new Property("location", "Oregon")))),
    (2.0, GBVertex(gbIds(3), gbIds(3), Set(new Property("location", "Oregon")))),
    (1.1, GBVertex(gbIds(4), gbIds(4), Set(new Property("location", "Oregon")))),
    (0.3, GBVertex(gbIds(5), gbIds(5), Set(new Property("location", "Oregon")))),
    (3.6, GBVertex(gbIds(6), gbIds(6), Set(new Property("location", "Oregon")))),
    (1.5, GBVertex(gbIds(7), gbIds(7), Set(new Property("location", "Oregon")))),
    (1.4, GBVertex(gbIds(8), gbIds(8), Set(new Property("location", "Oregon")))))

  val inputEdgeList = Seq(GBEdge(None, gbIds(1), gbIds(2), gbIds(1), gbIds(2), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(1), gbIds(3), gbIds(1), gbIds(3), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(1), gbIds(4), gbIds(1), gbIds(4), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(2), gbIds(1), gbIds(2), gbIds(1), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(2), gbIds(5), gbIds(2), gbIds(5), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(3), gbIds(1), gbIds(3), gbIds(1), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(3), gbIds(4), gbIds(3), gbIds(4), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(3), gbIds(6), gbIds(3), gbIds(6), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(3), gbIds(7), gbIds(3), gbIds(7), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(3), gbIds(8), gbIds(3), gbIds(8), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(4), gbIds(1), gbIds(4), gbIds(1), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(4), gbIds(3), gbIds(4), gbIds(3), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(5), gbIds(2), gbIds(5), gbIds(2), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(5), gbIds(6), gbIds(5), gbIds(6), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(5), gbIds(7), gbIds(5), gbIds(7), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(6), gbIds(3), gbIds(6), gbIds(3), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(6), gbIds(5), gbIds(6), gbIds(5), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(7), gbIds(3), gbIds(7), gbIds(3), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(7), gbIds(5), gbIds(7), gbIds(5), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
    GBEdge(None, gbIds(8), gbIds(3), gbIds(8), gbIds(3), "tweeted", Set(new Property("tweet", "blah blah blah..."))))

  "Generating a uniform vertex sample" should {

    "contain correct number of vertices in sample" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)

      val sampleVerticesRdd = VertexSampleSparkOps.sampleVerticesUniform(vertexRdd, 5, None)
      sampleVerticesRdd.count() shouldEqual 5
    }

    "give error if sample size less than 1" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)

      intercept[IllegalArgumentException] {
        VertexSampleSparkOps.sampleVerticesUniform(vertexRdd, 0, None)
      }
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)

      VertexSampleSparkOps.sampleVerticesUniform(vertexRdd, 200, None) shouldEqual vertexRdd
    }
  }

  "Generating a degree-weighted vertex sample" should {

    "contain correct number of vertices in sample" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val sampleVerticesRdd = VertexSampleSparkOps.sampleVerticesDegree(vertexRdd, edgeRdd, 5, None)
      sampleVerticesRdd.count() shouldEqual 5
    }

    "give error if sample size less than 1" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      intercept[IllegalArgumentException] {
        VertexSampleSparkOps.sampleVerticesDegree(vertexRdd, edgeRdd, 0, None)
      }
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      VertexSampleSparkOps.sampleVerticesDegree(vertexRdd, edgeRdd, 200, None) shouldEqual vertexRdd
    }
  }

  "Generating a degree distribution-weighted vertex sample" should {

    "contain correct number of vertices in sample" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val sampleVerticesRdd = VertexSampleSparkOps.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 5, None)
      sampleVerticesRdd.count() shouldEqual 5
    }

    "give error if sample size less than 1" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      intercept[IllegalArgumentException] {
        VertexSampleSparkOps.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 0, None)
      }
    }

    "returns entire dataset if sample size is greater than or equal to dataset size" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      VertexSampleSparkOps.sampleVerticesDegreeDist(vertexRdd, edgeRdd, 200, None) shouldEqual vertexRdd
    }
  }

  "Generating a vertex sample" should {

    "generate correct vertex induced subgraph" in {

      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val sampleVertexList = Seq(GBVertex(gbIds(1), gbIds(1), Set(new Property("location", "Oregon"))),
        GBVertex(gbIds(2), gbIds(2), Set(new Property("location", "Oregon"))),
        GBVertex(gbIds(3), gbIds(3), Set(new Property("location", "Oregon"))),
        GBVertex(gbIds(4), gbIds(4), Set(new Property("location", "Oregon"))))

      val sampleEdgeList = Seq(GBEdge(None, gbIds(1), gbIds(2), gbIds(1), gbIds(2), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
        GBEdge(None, gbIds(1), gbIds(3), gbIds(1), gbIds(3), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
        GBEdge(None, gbIds(1), gbIds(4), gbIds(1), gbIds(4), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
        GBEdge(None, gbIds(2), gbIds(1), gbIds(2), gbIds(1), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
        GBEdge(None, gbIds(3), gbIds(1), gbIds(3), gbIds(1), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
        GBEdge(None, gbIds(3), gbIds(4), gbIds(3), gbIds(4), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
        GBEdge(None, gbIds(4), gbIds(1), gbIds(4), gbIds(1), "tweeted", Set(new Property("tweet", "blah blah blah..."))),
        GBEdge(None, gbIds(4), gbIds(3), gbIds(4), gbIds(3), "tweeted", Set(new Property("tweet", "blah blah blah..."))))

      val sampleVertexRdd = sparkContext.parallelize(sampleVertexList, 2)
      val sampleEdgeRdd = sparkContext.parallelize(sampleEdgeList, 2)

      val subgraphEdges = VertexSampleSparkOps.vertexInducedEdgeSet(sampleVertexRdd, edgeRdd)

      subgraphEdges.count() shouldEqual sampleEdgeRdd.count()
      subgraphEdges.subtract(sampleEdgeRdd).count() shouldEqual 0
    }

    "correctly write the vertex induced subgraph to Titan" in new TestingTitan {
      setupTitan()
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val titanConnector = new TitanGraphConnector(titanConfig)

      VertexSampleSparkOps.writeToTitan(vertexRdd, edgeRdd, titanConfig)

      titanGraph = titanConnector.connect()

      titanGraph.getEdges.size shouldEqual 20
      TitanGraphConnector.getVertices(titanGraph).size shouldEqual 8 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+
      cleanupTitan()
    }

    "select the correct weighted vertices" in {
      val vertexRdd = sparkContext.parallelize(inputVertexListWeighted, 2)

      val topVertexRdd = VertexSampleSparkOps.getTopVertices(vertexRdd, 4)
      val topVertexArray = topVertexRdd.collect()

      topVertexArray.contains(inputVertexListWeighted(5)._2) shouldEqual true
      topVertexArray.contains(inputVertexListWeighted(2)._2) shouldEqual true
      topVertexArray.contains(inputVertexListWeighted(6)._2) shouldEqual true
      topVertexArray.contains(inputVertexListWeighted(7)._2) shouldEqual true
    }
  }

  "Degree weighted sampling" should {

    "add correct vertex weights" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val weightedVertexRdd = VertexSampleSparkOps.addVertexDegreeWeights(vertexRdd, edgeRdd)
      val weightedVertexArray = weightedVertexRdd.take(8).map { case (weight, vertex) => (vertex, weight) }.toMap

      weightedVertexArray(inputVertexList(0)) shouldEqual 3l
      weightedVertexArray(inputVertexList(1)) shouldEqual 2l
      weightedVertexArray(inputVertexList(2)) shouldEqual 5l
      weightedVertexArray(inputVertexList(3)) shouldEqual 2l
      weightedVertexArray(inputVertexList(4)) shouldEqual 3l
      weightedVertexArray(inputVertexList(5)) shouldEqual 2l
      weightedVertexArray(inputVertexList(6)) shouldEqual 2l
      weightedVertexArray(inputVertexList(7)) shouldEqual 1l
    }
  }

  "DegreeDist weighted sampling" should {

    "add correct vertex weights" in {
      val vertexRdd = sparkContext.parallelize(inputVertexList, 2)
      val edgeRdd = sparkContext.parallelize(inputEdgeList, 2)

      val weightedVertexRdd = VertexSampleSparkOps.addVertexDegreeDistWeights(vertexRdd, edgeRdd)
      val weightedVertexArray = weightedVertexRdd.take(8).map { case (weight, vertex) => (vertex, weight) }.toMap

      weightedVertexArray(inputVertexList(0)) shouldEqual 2l
      weightedVertexArray(inputVertexList(1)) shouldEqual 4l
      weightedVertexArray(inputVertexList(2)) shouldEqual 1l
      weightedVertexArray(inputVertexList(3)) shouldEqual 4l
      weightedVertexArray(inputVertexList(4)) shouldEqual 2l
      weightedVertexArray(inputVertexList(5)) shouldEqual 4l
      weightedVertexArray(inputVertexList(6)) shouldEqual 4l
      weightedVertexArray(inputVertexList(7)) shouldEqual 1l
    }
  }

}
