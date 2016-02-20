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
package org.trustedanalytics.atk.engine.frame.plugins.topk

import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.frame.plugins.statistics.NumericValidationUtils
import org.trustedanalytics.atk.engine.frame.plugins.statistics.descriptives.ColumnStatistics
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.PriorityQueue

/**
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private object TopKRddFunctions extends Serializable {

  case class CountPair(key: Any, value: Double) extends Ordered[CountPair] {
    def compare(that: CountPair) = this.value compare that.value
  }

  /**
   * Returns the top (or bottom) K distinct values by count for specified data column.
   *
   * @param frameRdd RDD for data frame
   * @param dataColumnIndex Index of data column
   * @param k Number of entries to return
   * @param useBottomK Return bottom K entries if true, else return top K
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @return Top (or bottom) K distinct values by count for specified column
   */
  def topK(frameRdd: RDD[Row], dataColumnIndex: Int, k: Int, useBottomK: Boolean = false,
           weightsColumnIndexOption: Option[Int] = None,
           weightsTypeOption: Option[DataType] = None): RDD[Row] = {
    require(dataColumnIndex >= 0, "label column index must be greater than or equal to zero")

    val dataWeightPairs =
      ColumnStatistics.getDataWeightPairs(dataColumnIndex, weightsColumnIndexOption, weightsTypeOption, frameRdd)
        .filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val distinctCountRDD = dataWeightPairs.reduceByKey((a, b) => a + b)

    //Sort by descending order to get top K
    val isDescendingSort = !useBottomK

    // Efficiently get the top (or bottom) K entries by first sorting the top (or bottom) K entries in each partition
    // This function uses a tree map instead of a bounded priority queue (despite the added overhead)
    // because we need to keep key-value pairs
    val topKByPartition = distinctCountRDD.mapPartitions(countIterator => {
      Iterator(sortTopKByValue(countIterator, k, isDescendingSort))
    }).reduce({ (topPartition1, topPartition2) =>
      mergeTopKSortedSeqs(topPartition1, topPartition2, isDescendingSort, k)
    })

    // Get the overall top (or bottom) K entries from partitions
    // Works when K*num_partitions fits in memory of single machine.
    val topRows = topKByPartition.map(f => Row(f.key, f.value))
    frameRdd.sparkContext.parallelize(topRows)
  }

  /**
   * Returns top K entries sorted by value.
   *
   * The sort ordering may either be ascending or descending.
   *
   * @param inputIterator Iterator of key-value pairs to sort
   * @param k Number of top sorted entries to return
   * @param descending Sort in descending order if true, else sort in ascending order
   * @return Top K sorted entries
   */
  def sortTopKByValue(inputIterator: Iterator[(Any, Double)],
                      k: Int, descending: Boolean = false): Seq[CountPair] = {
    val ordering = if (descending) Ordering[CountPair].reverse else Ordering[CountPair]
    val priorityQueue = new PriorityQueue[CountPair]()(ordering)

    inputIterator.foreach(element => {
      priorityQueue.enqueue(CountPair(element._1, element._2))
      if (priorityQueue.size > k) priorityQueue.dequeue()
    })

    priorityQueue.reverse.dequeueAll
  }

  /**
   * Merge two sorted sequences while maintaining sort order, and return topK.
   *
   * @param sortedSeq1 First sorted sequence
   * @param sortedSeq2 Second sorted sequence
   * @param descending Sort in descending order if true, else sort in ascending order
   * @param k Number of top sorted entries to return
   * @return Merged sorted sequence with topK entries
   */
  private def mergeTopKSortedSeqs(sortedSeq1: Seq[CountPair], sortedSeq2: Seq[CountPair],
                                  descending: Boolean = false, k: Int): Seq[CountPair] = {
    // Previously tried recursive and non-recursive merge but Scala sort turned out to be the fastest.
    // Recursive merge was overflowing the stack for large K
    val ordering = if (descending) Ordering[CountPair].reverse else Ordering[CountPair]
    (sortedSeq1 ++ sortedSeq2).sorted(ordering).take(k)

  }
}
