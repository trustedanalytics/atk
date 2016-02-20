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
package org.trustedanalytics.atk.engine.frame.plugins.sortedk

import org.apache.spark.frame.FrameRdd
import org.apache.spark.frame.ordering.{ FrameOrderingUtils, MultiColumnKeyOrdering }

/**
 * Object with methods for sorting the top-k rows in a frame
 */
object SortedKFunctions extends Serializable {

  /**
   * Return the top-K rows in a frame ordered by column(s).
   *
   * @param frameRdd Frame to sort
   * @param k Number of sorted records to return
   * @param columnNamesAndAscending Column names to sort by, true for ascending, false for descending
   * @param reduceTreeDepth Depth of reduce tree (governs number of rounds of reduce tasks)
   * @return New frame with top-K rows
   */
  def takeOrdered(frameRdd: FrameRdd,
                  k: Int,
                  columnNamesAndAscending: List[(String, Boolean)],
                  reduceTreeDepth: Option[Int] = None): FrameRdd = {
    require(k > 0, "k should be greater than zero") //TODO: Should we add an upper bound for K
    require(columnNamesAndAscending != null && columnNamesAndAscending.nonEmpty, "one or more columnNames is required")

    val columnNames = columnNamesAndAscending.map(_._1)
    val ascendingPerColumn = columnNamesAndAscending.map(_._2)

    val pairRdd = frameRdd.mapRows(row => (row.values(columnNames), row.data))
    implicit val keyOrdering = new MultiColumnKeyOrdering(ascendingPerColumn)

    val topRows = if (reduceTreeDepth.isDefined) {
      FrameOrderingUtils.takeOrderedTree(pairRdd, k, reduceTreeDepth.get)
    }
    else {
      FrameOrderingUtils.takeOrderedTree(pairRdd, k)
    }

    // ascending is always true here because we control in the ordering
    val topRowsRdd = frameRdd.sparkContext.parallelize(topRows).map { case (key, row) => row }
    new FrameRdd(frameRdd.frameSchema, topRowsRdd)
  }
}
