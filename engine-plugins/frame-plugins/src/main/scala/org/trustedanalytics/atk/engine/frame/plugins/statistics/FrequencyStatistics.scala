/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.engine.frame.plugins.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd._

/**
 * Object for calculating the frequency statistics of a collection of (data,weight) pairs, represented as an
 * RDD of (T,Double) pairs, where T is a type parameter.
 *
 * All data items with weights <= 0 are excluded from these calculations.
 *
 * @param dataWeightPairs RDD containing pairs (data, weight) where the each "data" entry is unique.
 * @param maxNumberOfModesReturned Maximum number of items returned in the mode set.
 * @tparam T Value type.
 */
class FrequencyStatistics[T: ClassManifest](dataWeightPairs: RDD[(T, Double)], maxNumberOfModesReturned: Int)
    extends Serializable {

  /**
   * Set of at most maxNumberOfModesReturned modes. A mode is an item with maximum weight.
   * If there is no item with positive weight, the returned set is empty.
   */
  lazy val modeSet: Set[T] = modeStatistics.modes.toSet[T]

  /**
   * The weight of a mode of the input. It is either strictly positive, or,
   * if there is no item with positive weight, weightOfMode is 0 .
   */
  lazy val weightOfMode: Double = modeStatistics.weightOfMode

  /**
   * The number of modes in the data.
   */
  lazy val modeCount: Long = modeStatistics.modeCount

  /**
   * Sum of all weights.
   */
  lazy val totalWeight: Double = modeStatistics.totalWeight

  private lazy val modeStatistics: ModeStatsCounter[T] = generateMode(maxNumberOfModesReturned)

  private def generateMode(maxNumberOfModesReturned: Int): ModeStatsCounter[T] = {

    val acumulatorParam = new FrequencyStatsAccumulatorParam[T](maxNumberOfModesReturned)
    val initialValue = ModeStatsCounter[T](List.empty[T], 0, 0, 0)

    val accumulator =
      dataWeightPairs.sparkContext.accumulator[ModeStatsCounter[T]](initialValue)(acumulatorParam)

    val dataWeightPairsPositiveWeights =
      dataWeightPairs.filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val uniqueValuesPositiveWeights: RDD[(T, Double)] =
      dataWeightPairsPositiveWeights.reduceByKey(_+_)

    uniqueValuesPositiveWeights.foreach(
      {
        case (value, weightAtValue) =>
          accumulator.add(ModeStatsCounter(List(value), weightAtValue, weightAtValue, 1))
      })

    ModeStatsCounter[T](accumulator.value.modes,
      accumulator.value.weightOfMode,
      accumulator.value.totalWeight,
      accumulator.value.modeCount)

  }

}

/*
 * Class for accumulating frequency statistics in one pass over the data.
 * @param mode Set of <= k modes seen so far.
 * @param weightOfMode The weight of the mode. 0 when run over empty data.
 * @param totalWeight Sum of the weights of all values seen so far.
 * @param modeCount The number of distinct modes seen so far.
 * @tparam T Type of the input data. (In particular, the type of the mode.)
 */
private case class ModeStatsCounter[T](modes: List[T], weightOfMode: Double, totalWeight: Double, modeCount: Long)
  extends Serializable

/*
 * Configures the spark accumulator for gathering frequency statistics.
 * @tparam T The type of the input data.
 * @param maxNumberOfModesReturned The maximum number of modes to track in the accumulator.
 */
private class FrequencyStatsAccumulatorParam[T](maxNumberOfModesReturned: Int)
    extends AccumulatorParam[ModeStatsCounter[T]] with Serializable {

  private val ordering = new canonicalOrdering[T]

  override def zero(initialValue: ModeStatsCounter[T]) = ModeStatsCounter(List.empty[T], 0, 0, 0)

  override def addInPlace(stats1: ModeStatsCounter[T], stats2: ModeStatsCounter[T]): ModeStatsCounter[T] = {
    if (stats1.modes.isEmpty) {
      stats2
    }
    else if (stats2.modes.isEmpty) {
      stats1
    }
    else {
      if (stats1.weightOfMode > stats2.weightOfMode) {
        ModeStatsCounter(stats1.modes, stats1.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats1.modeCount)
      }
      else if (stats1.weightOfMode < stats2.weightOfMode) {
        ModeStatsCounter(stats2.modes,
          stats2.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats2.modeCount)
      }
      else {
        val kLeastModes = merge(stats1.modes, stats2.modes, maxNumberOfModesReturned)(ordering)
        ModeStatsCounter(kLeastModes,
          stats1.weightOfMode,
          stats1.totalWeight + stats2.totalWeight,
          stats1.modeCount + stats2.modeCount)
      }
    }
  }

  private def merge(list1: List[T], list2: List[T], k: Int)(implicit order: Ordering[T]): List[T] = {
    if (k <= 0) {
      List.empty[T]
    }
    else if (list1.isEmpty) {
      list2.take(k)
    }
    else if (list2.isEmpty) {
      list1.take(k)
    }
    else if (order.lt(list1.head, list2.head)) {
      list1.head :: merge(list1.drop(1), list2, k - 1)
    }
    else {
      list2.head :: merge(list1, list2.drop(1), k - 1)
    }
  }

  // A canonical ordering so we can sure that the sets of modes returned are reproducible for the same data.
  // TODO the right solution for this is to:
  //  have rows lug around a container for TA datatypes, not "Any" and then just have the comparators defined
  //  for all the subclasses of TA datatypes.... but that might be a long way off

  private class canonicalOrdering[T] extends Ordering[T] {
    def compare(a: T, b: T) = {
      a match {
        case i: Int => i.compareTo(b.asInstanceOf[Int])
        case l: Long => l.compareTo(b.asInstanceOf[Long])
        case fl: Float => fl.compareTo(b.asInstanceOf[Float])
        case d: Double => d.compareTo(b.asInstanceOf[Double])
        case s: String => s.compareTo(b.asInstanceOf[String])
        case _ =>
          throw new IllegalArgumentException("Attempt to get frequency statistics for unsupported datatype: "
            + a.getClass.getName)
      }
    }
  }
}
