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

import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex }
import org.trustedanalytics.atk.graphbuilder.parser.Parser
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, TaskContext }

import scala.collection.mutable.Map

/**
 * Parse the raw rows of input into Vertices
 *
 * @param vertexParser the parser to use
 */
class VertexParserRdd(prev: RDD[Seq[_]], vertexParser: Parser[GBVertex]) extends RDD[GBVertex](prev) {

  override def getPartitions: Array[Partition] = firstParent[GBVertex].partitions

  /**
   * Parse the raw rows of input into Vertices
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GBVertex] = {

    // In some data sets many vertices are duplicates and many of the duplicates are
    // 'near' each other in the parsing process (like Netflix movie data where the
    // rows define both edges and vertices and the rows are ordered by user id). By
    // keeping a map and merging the duplicates that occur in a given split, there
    // will be less to deal with later. This is like a combiner in Hadoop Map/Reduce,
    // it won't remove all duplicates in the final RDD but there will be less to
    // shuffle later.  For input without duplicates, this shouldn't add much overhead.
    val vertexMap = Map[Property, GBVertex]()

    firstParent[Seq[_]].iterator(split, context).foreach(row => {
      vertexParser.parse(row).foreach(v => {
        val opt = vertexMap.get(v.gbId)
        if (opt.isDefined) {
          vertexMap.put(v.gbId, v.merge(opt.get))
        }
        else {
          vertexMap.put(v.gbId, v)
        }
      })
    })

    vertexMap.valuesIterator
  }
}
