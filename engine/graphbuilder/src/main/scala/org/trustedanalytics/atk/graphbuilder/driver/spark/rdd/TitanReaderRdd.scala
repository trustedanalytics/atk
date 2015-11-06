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

package org.trustedanalytics.atk.graphbuilder.driver.spark.rdd

import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.util.TitanConverter
import org.trustedanalytics.atk.graphbuilder.elements.GraphElement
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import com.thinkaurelius.titan.hadoop.FaunusVertex
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{ InterruptibleIterator, Partition, TaskContext }

/**
 * RDD that loads Titan graph from HBase.
 *
 * @param faunusRdd Input RDD
 * @param titanConnector connector to Titan
 */

class TitanReaderRdd(faunusRdd: RDD[(NullWritable, FaunusVertex)], titanConnector: TitanGraphConnector) extends RDD[GraphElement](faunusRdd) {

  override def getPartitions: Array[Partition] = firstParent[(NullWritable, FaunusVertex)].partitions

  /**
   * Parses HBase input rows to extract vertices and corresponding edges.
   *
   * @return Iterator of GraphBuilder vertices and edges using GraphBuilder's GraphElement trait
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GraphElement] = {

    val graphElements = firstParent[(NullWritable, FaunusVertex)].iterator(split, context).flatMap(inputRow => {
      val faunusVertex = inputRow._2

      val gbVertex = TitanConverter.createGraphBuilderVertex(faunusVertex)
      val gbEdges = TitanConverter.createGraphBuilderEdges(faunusVertex)

      val rowGraphElements: Iterator[GraphElement] = Iterator(gbVertex) ++ gbEdges

      rowGraphElements
    })

    new InterruptibleIterator(context, graphElements)
  }

}
