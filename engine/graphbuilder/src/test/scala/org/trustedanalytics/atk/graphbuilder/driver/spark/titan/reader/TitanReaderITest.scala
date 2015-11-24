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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader

import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.TitanReaderRdd
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.reader.TitanReaderTestData._
import org.trustedanalytics.atk.graphbuilder.elements.{ GBVertex, GBEdge, GraphElement }
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.NullWritable
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

import scala.collection.JavaConversions._

/**
 * End-to-end integration test for Titan reader
 * @todo Use Stephen's TestingSparkContext class for scalatest
 */
class TitanReaderITest extends TestingSparkContextWordSpec with Matchers {

  "Reading a Titan graph from HBase should" should {
    "return an empty list of graph elements if the HBase table is empty" in {
      val hBaseRDD = sparkContext.parallelize(Seq.empty[(NullWritable, FaunusVertex)])
      val titanReaderRDD = new TitanReaderRdd(hBaseRDD, titanConnector)
      val graphElements = titanReaderRDD.collect()
      graphElements.length shouldBe 0
    }
    "return 3 GraphBuilder vertices and 2 GraphBuilder rows" in {
      val hBaseRDD = sparkContext.parallelize(
        Seq((NullWritable.get(), neptuneFaunusVertex),
          (NullWritable.get(), plutoFaunusVertex),
          (NullWritable.get(), seaFaunusVertex)))

      val titanReaderRDD = new TitanReaderRdd(hBaseRDD, titanConnector)
      val vertexRDD = titanReaderRDD.filterVertices()
      val edgeRDD = titanReaderRDD.filterEdges()

      val graphElements = titanReaderRDD.collect()
      val vertices = vertexRDD.collect()
      val edges = edgeRDD.collect()

      graphElements.length shouldBe 5
      vertices.length shouldBe 3
      edges.length shouldBe 2

      graphElements.map {
        case v: GBVertex => v
        case e: GBEdge => e.copy(eid = None)
      } should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex, plutoGbEdge, seaGbEdge)
      vertices should contain theSameElementsAs List[GraphElement](plutoGbVertex, seaGbVertex, neptuneGbVertex)
      edges.map(e => e.copy(eid = None)) should contain theSameElementsAs List[GraphElement](plutoGbEdge, seaGbEdge)
    }
  }
}
