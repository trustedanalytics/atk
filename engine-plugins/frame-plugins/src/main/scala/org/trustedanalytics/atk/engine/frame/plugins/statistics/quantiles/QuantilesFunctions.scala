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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.quantiles

import org.trustedanalytics.atk.domain.frame.{ QuantileComposingElement, QuantileTarget }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.MiscFrameFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

/**
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object QuantilesFunctions extends Serializable {

  /**
   * Calculate quantile values
   *
   * Currently calculate quantiles with weight average. n be the number of total elements which is ordered,
   * T th quantile can be calculated in the following way.
   * n * T / 100 = i + j   i is the integer part and j is the fractional part
   * The quantile is Xi * (1- j) + Xi+1 * j
   *
   * Calculating a list of quantiles follows the following process:
   * 1. calculate components for each quantile. If T th quantile is Xi * (1- j) + Xi+1 * j, output
   * (i, (1 - j)), (i + 1, j).
   * 2. transform the components. Take component (i, (1 - j)) and transform to (i, (T, 1 - j)), where (T, 1 -j) is
   * a quantile target for element i. Create mapping i -> seq(quantile targets)
   * 3. iterate through all elements in each partition. for element i, find sequence of quantile targets from
   * the mapping created earlier. emit (T, i * (1 - j))
   * 4. reduce by key, which is quantile. Sum all partial results to get the final quantile values.
   *
   * @param rdd input rdd
   * @param quantiles seq of quantiles to find value for
   * @param columnIndex the index of column to calculate quantile
   * @param rowCount number of records found in RDD. If set to none this will perform an rdd.count to determine
   */
  def quantiles(rdd: RDD[Row], quantiles: Seq[Double], columnIndex: Int, rowCount: Long): RDD[Row] = {
    val singleColumn = rdd.map(row => DataTypes.toDouble(row(columnIndex)))
    val sorted = singleColumn.sortBy(x => x)

    val quantileTargetMapping = getQuantileTargetMapping(rowCount, quantiles)
    val sumsAndCounts: Map[Int, (Long, Long)] = MiscFrameFunctions.getPerPartitionCountAndAccumulatedSum(sorted)

    //this is the first stage of calculating quantile
    //generate data that has keys as quantiles and values as column data times weight
    val quantilesComponentsRDD = sorted.mapPartitionsWithIndex((partitionIndex, values) => {
      var rowIndex: Long = (if (partitionIndex == 0) 0 else sumsAndCounts(partitionIndex - 1)._2) + 1
      val perPartitionResult = ListBuffer[(Double, BigDecimal)]()

      for (value <- values) {
        if (quantileTargetMapping.contains(rowIndex)) {
          val targets: Seq[QuantileTarget] = quantileTargetMapping(rowIndex)

          for (quantileTarget <- targets) {
            val numericVal = DataTypes.toBigDecimal(value)
            perPartitionResult += ((quantileTarget.quantile, value * quantileTarget.weight))
          }
        }

        rowIndex = rowIndex + 1
      }

      perPartitionResult.toIterator
    })

    quantilesComponentsRDD.reduceByKey(_ + _).sortByKey(ascending = true).map(pair => new GenericRow(Array[Any](pair._1, pair._2.toDouble)))
  }

  /**
   * calculate and return elements for calculating quantile
   * For example, 25th quantile out of 10 rows(X1, X2, X3, ... X10) will be
   * 0.5 * x2 + 0.5 * x3. The method will return x2 and x3 with weight as 0.5
   *
   * For whole quantile calculation process, please refer to doc of method calculateQuantiles
   * @param totalRows
   * @param quantile
   */
  def getQuantileComposingElements(totalRows: Long, quantile: Double): Seq[QuantileComposingElement] = {
    val position = (BigDecimal(quantile) * totalRows) / 100
    var integerPosition = position.toLong
    val fractionPosition = position - integerPosition

    val result = mutable.ListBuffer[QuantileComposingElement]()

    val addQuantileComposingElement = (position: Long, quantile: Double, weight: BigDecimal) => {
      //element starts from 1. therefore X0 equals X1
      if (weight > 0)
        result += QuantileComposingElement(if (position != 0) position else 1, QuantileTarget(quantile, weight))
    }

    addQuantileComposingElement(integerPosition, quantile, 1 - fractionPosition)
    addQuantileComposingElement(integerPosition + 1, quantile, fractionPosition)
    result.toSeq
  }

  /**
   * Calculate mapping between an element's position and Seq of quantiles that the element can contribute to
   *
   * For whole quantile calculation process, please refer to doc of method calculateQuantiles
   *
   * @param totalRows total number of rows in the data
   * @param quantiles Sequence of quantiles to search
   */
  def getQuantileTargetMapping(totalRows: Long, quantiles: Seq[Double]): Map[Long, Seq[QuantileTarget]] = {

    val composingElements: Seq[QuantileComposingElement] = quantiles.flatMap(quantile => getQuantileComposingElements(totalRows, quantile))

    val mapping = mutable.Map[Long, ListBuffer[QuantileTarget]]()
    for (element <- composingElements) {
      val elementIndex: Long = element.index

      if (!mapping.contains(elementIndex))
        mapping(elementIndex) = ListBuffer[QuantileTarget]()

      mapping(elementIndex) += element.quantileTarget
    }

    //for each element's quantile targets, convert from ListBuffer to Seq
    //convert the map to immutable map
    mapping.map { case (elementIndex, targets) => (elementIndex, targets.toSeq) }.toMap
  }

}
