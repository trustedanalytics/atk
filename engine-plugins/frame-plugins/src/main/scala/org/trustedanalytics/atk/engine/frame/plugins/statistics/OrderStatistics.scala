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


package org.trustedanalytics.atk.engine.frame.plugins.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.reflect.ClassTag

/**
 * Calculate order statistics for any weighted RDD of data that provides an ordering function.
 * @param dataWeightPairs RDD of (data, weight) pairs.
 * @param ordering Ordering on the data items.
 * @tparam T The type of the data objects. It must have an ordering function in scope.
 */
class OrderStatistics[T: ClassTag](dataWeightPairs: RDD[(T, Double)])(implicit ordering: Ordering[T])
    extends Serializable {

  /**
   * Option containing the median of the input distribution. The median is the least value X in the range of the
   * distribution so that the cumulative weight strictly below X is < 1/2  the total weight and the cumulative
   * distribution up to and including X is >= 1/2 the total weight.
   *
   * Values with non-positive weights are thrown out before the calculation is performed.
   * The option None is returned when the total weight is 0.
   */
  lazy val medianOption: Option[T] = computeMedian

  /*
   * Computes the median via a sort and scan approach, although the nature of RDDs greatly complicates the "simple scan"
   *
   * TODO: investigate the use of a sort-free (aka "linear time") median algorithm, TRIB-3151
   */
  private def computeMedian: Option[T] = {

    val sortedDataWeightPairs: RDD[(T, BigDecimal)] =
      dataWeightPairs.filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) }).
        map({ case (data, weight) => (data, BigDecimal(weight)) }).sortByKey(ascending = true)

    val weightsOfPartitions: Array[BigDecimal] = sortedDataWeightPairs.mapPartitions(sumWeightsInPartition).collect()

    val totalWeight: BigDecimal = weightsOfPartitions.sum

    if (totalWeight <= 0) {
      None
    }
    else {

      // the "median partition" is the partition the contains the median
      val (indexOfMedianPartition, weightInPrecedingPartitions) = findMedianPartition(weightsOfPartitions, totalWeight)

      val median: T = sortedDataWeightPairs.mapPartitionsWithIndex({
        case (partitionIndex, iterator) =>
          if (partitionIndex != indexOfMedianPartition) Iterator.empty
          else medianInSingletonIterator[T](iterator, totalWeight, weightInPrecedingPartitions)
      }).collect().head

      Some(median)
    }
  }

  // Sums the weights in an individual partition of an RDD[(T, BigDecimal)] where second coordinate is "weight'
  private def sumWeightsInPartition(it: Iterator[(T, BigDecimal)]): Iterator[BigDecimal] =
    if (it.nonEmpty) Iterator(it.map({ case (x, w) => w }).sum) else Iterator(0)

  // take an iterator for the partition that contains the median, and returns the median...
  // as the single element in a new iterator
  private def medianInSingletonIterator[T](it: Iterator[(T, BigDecimal)],
                                           totalWeight: BigDecimal,
                                           weightInPrecedingPartitions: BigDecimal): Iterator[T] = {

    val weightPrecedingMedian = (totalWeight / 2) - weightInPrecedingPartitions

    if (it.nonEmpty) {
      var currentDataWeightPair: (T, BigDecimal) = it.next()
      var weightSoFar: BigDecimal = currentDataWeightPair._2

      while (weightSoFar < weightPrecedingMedian) {
        currentDataWeightPair = it.next()
        weightSoFar += currentDataWeightPair._2
      }
      Iterator(currentDataWeightPair._1)
    }
    else {
      Iterator.empty
    }
  }

  // Find the index of the partition that contains the median of the of the dataset, as well as the net weight of the
  // partitions that precede it.
  private def findMedianPartition(weightsOfPartitions: Array[BigDecimal], totalWeight: BigDecimal): (Int, BigDecimal) = {
    var currentPartition: Int = 0
    var weightInPrecedingPartitions: BigDecimal = 0
    while (weightInPrecedingPartitions + weightsOfPartitions(currentPartition) < totalWeight / 2) {
      weightInPrecedingPartitions += weightsOfPartitions(currentPartition)
      currentPartition += 1
    }
    (currentPartition, weightInPrecedingPartitions)
  }

}
