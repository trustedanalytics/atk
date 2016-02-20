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

package org.trustedanalytics.atk.engine.frame.plugins.cumulativedist

import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column }
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

/**
 * Functions for computing various types of cumulative distributions
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object CumulativeDistFunctions extends Serializable {

  /**
   * Generate the empirical cumulative distribution for an input dataframe column
   *
   * @param frameRdd rdd for a Frame
   * @param sampleColumn column containing the sample data
   * @return a new RDD of tuples containing each distinct sample value and its ecdf value
   */
  def ecdf(frameRdd: FrameRdd, sampleColumn: Column): RDD[Row] = {
    // get distribution of values
    val sortedRdd = frameRdd.mapRows(row => {
      val sample = row.doubleValue(sampleColumn.name)
      (sample, 1)
    }).reduceByKey(_ + _).sortByKey().cache()

    // compute the partition sums
    val partSums: Array[Double] = 0.0 +: sortedRdd.mapPartitionsWithIndex {
      case (index, partition) => Iterator(partition.map { case (sample, count) => count }.sum.toDouble)
    }.collect()

    // get sample size
    val numValues = partSums.sum

    // compute empirical cumulative distribution
    val sumsRdd = sortedRdd.mapPartitionsWithIndex {
      case (index, partition) => {
        var startValue = 0.0
        for (i <- 0 to index) {
          startValue += partSums(i)
        }
        partition.scanLeft((0.0, startValue))((prev, curr) => (curr._1, prev._2 + curr._2)).drop(1)
      }
    }

    sumsRdd.map {
      case (value, valueSum) => {
        sampleColumn.dataType match {
          case DataTypes.int32 => Row(value.toInt, valueSum / numValues)
          case DataTypes.int64 => Row(value.toLong, valueSum / numValues)
          case DataTypes.float32 => Row(value.toFloat, valueSum / numValues)
          case DataTypes.float64 => Row(value.toDouble, valueSum / numValues)
          case _ => Row(value, valueSum / numValues)
        }
      }
    }
  }

  /**
   * Compute the cumulative sum of the input frameRdd for the specified column index
   *
   * @param frameRdd input frame RDD
   * @param sampleColumnName name of the column to compute cumulative sum
   * @return an RDD of tuples containing (originalValue, cumulativeSumAtThisValue)
   */
  def cumulativeSum(frameRdd: FrameRdd, sampleColumnName: String): RDD[Row] = {

    cumulativeCountWithRevertTypes(pairedRdd(frameRdd, sampleColumnName))
  }

  /**
   * Compute the cumulative count of the input frameRdd for the specified column index
   *
   * @param frameRdd input frame RDD
   * @param sampleColumnName name of the column to compute cumulative count
   * @param countValue the value to count
   * @return an RDD of tuples containing (originalValue, cumulativeCountAtThisValue)
   */
  def cumulativeCount(frameRdd: FrameRdd, sampleColumnName: String, countValue: String): RDD[Row] = {

    cumulativeCountWithRevertTypes(pairedRdd(frameRdd, sampleColumnName, countValue))
  }

  /**
   * Compute the cumulative percent sum of the input frameRdd for the specified column index
   *
   * @param frameRdd input frame RDD
   * @param sampleColumnName name of the column to compute cumulative percent sum
   * @return an RDD of tuples containing (originalValue, cumulativePercentSumAtThisValue)
   */
  def cumulativePercentSum(frameRdd: FrameRdd, sampleColumnName: String): RDD[Row] = {

    cumulativeCountWithRevertPercentTypes(pairedRdd(frameRdd, sampleColumnName))
  }

  /**
   * Compute the cumulative percent count of the input frameRdd for the specified column index
   *
   * @param frameRdd input frame RDD
   * @param sampleColumnName name of the column to compute cumulative percent count
   * @param countValue the value to count
   * @return an RDD of tuples containing (originalValue, cumulativePercentCountAtThisValue)
   */
  def cumulativePercentCount(frameRdd: FrameRdd, sampleColumnName: String, countValue: String): RDD[Row] = {

    cumulativeCountWithRevertPercentTypes(pairedRdd(frameRdd, sampleColumnName, countValue))
  }

  /**
   * Builds a pairedRdd from frame
   *
   * @param frameRdd input frame RDD
   * @param sampleColumnName a column name for the output values
   * @return an of Row and the values from the sampleColumnName
   */
  def pairedRdd(frameRdd: FrameRdd, sampleColumnName: String): RDD[(Row, Double)] = {

    frameRdd.mapRows(rowWrapper => (rowWrapper.row, rowWrapper.doubleValue(sampleColumnName)))
  }

  /**
   * Compute the cumulative count of the input frameRdd for the specified column index
   *
   * @param pairedRdd input paired RDD
   * @return an RDD of tuples containing (originalValue, cumulativeCountAtThisValue)
   */
  def cumulativeCountAsPairedRDD(pairedRdd: RDD[(Row, Double)]): RDD[(Row, Double)] = {

    totalPartitionSums(pairedRdd, partitionSums(pairedRdd.values))
  }

  /**
   * Calculates the sum of a numeric column
   * @param rdd input frame
   * @return the sum as double
   */
  def columnSum(rdd: RDD[(Double)]): Double = {

    partitionSums(rdd).sum
  }

  /**
   * Calculates the sum of a numeric column
   * @param frameRdd input frame
   * @param columnName column name
   * @return the sum as double
   */
  def columnSum(frameRdd: FrameRdd, columnName: String): Double = {

    columnSum(pairedRdd(frameRdd, columnName).values)
  }

  /**
   * Compute the cumulative count of the input frameRdd for the specified column index
   *
   * @param pairedRdd input paired RDD
   * @return an RDD of tuples containing (originalValue, cumulativeCountAtThisValue)
   */
  private def cumulativeCountWithRevertPercentTypes(pairedRdd: RDD[(Row, Double)]): RDD[Row] = {

    val cumulativeCounts = cumulativeCountAsPairedRDD(pairedRdd)
    val numValues = pairedRdd.map { case (row, columnValue) => columnValue }.sum()

    revertPercentTypes(cumulativeCounts, numValues)
  }

  /**
   * Compute the cumulative count of the input frameRdd for the specified column index
   *
   * @param pairedRdd input paired RDD
   * @return an RDD of tuples containing (originalValue, cumulativeCountAtThisValue)
   */
  private def cumulativeCountWithRevertTypes(pairedRdd: RDD[(Row, Double)]): RDD[Row] = {

    val cumulativeCounts = cumulativeCountAsPairedRDD(pairedRdd)

    revertTypes(cumulativeCounts)
  }

  /**
   * Builds a pairedRdd from frame
   *
   * @param frameRdd input frame RDD
   * @param sampleColumnName column name whose values will be used to calculate the output pairedRdd
   * @param countValue a count value
   * @return an RDD of row and 0.0 or 1.0
   */
  private def pairedRdd(frameRdd: FrameRdd, sampleColumnName: String, countValue: String): RDD[(Row, Double)] = {

    frameRdd.mapRows(rowWrapper => {
      val sampleValue = rowWrapper.stringValue(sampleColumnName)
      if (sampleValue.equals(countValue)) {
        (rowWrapper.row, 1.0)
      }
      else {
        (rowWrapper.row, 0.0)
      }
    })
  }

  /**
   * Compute the sum for each partition in RDD
   *
   * @param rdd the input RDD
   * @return an Array[Double] that contains the partition sums
   */
  private[cumulativedist] def partitionSums(rdd: RDD[Double]): Array[Double] = {
    0.0 +: rdd.mapPartitionsWithIndex {
      case (index, partition) => Iterator(partition.sum)
    }.collect()
  }

  /**
   * Compute the total sums across partitions
   *
   * @param rdd the input RDD
   * @param partSums the sums for each partition
   * @return RDD of (value, cumulativeSum)
   */
  private def totalPartitionSums(rdd: RDD[(Row, Double)], partSums: Array[Double]): RDD[(Row, Double)] = {
    rdd.mapPartitionsWithIndex({
      (index, partition) =>
        var startValue = 0.0
        for (i <- 0 to index) {
          startValue += partSums(i)
        }
        // startValue updated, so drop first value
        partition.scanLeft((Row(), startValue))((prev, curr) => (curr._1, prev._2 + curr._2)).drop(1)
    }, preservesPartitioning = true
    )
  }

  /**
   * Casts the input data back to the original input type
   *
   * @param rdd the RDD containing (value, cumulativeDistValue)
   * @return RDD containing Array[Any] (i.e., Rows)
   */
  private def revertTypes(rdd: RDD[(Row, Double)]): RDD[Row] = {
    rdd.map {
      case (row, valueSum) =>
        Row.fromSeq(row.toSeq :+ valueSum)
    }
  }

  /**
   * Casts the input data for cumulative percents back to the original input type.  This includes check for
   * divide-by-zero error.
   *
   * @param rdd the RDD containing (value, cumulativeDistValue)
   * @param numValues number of values in the user-specified column
   * @return RDD containing Array[Any] (i.e., Rows)
   */
  private def revertPercentTypes(rdd: RDD[(Row, Double)], numValues: Double): RDD[Row] = {
    rdd.map {
      case (row, valueSum) => {
        numValues match {
          case 0 => Row.fromSeq(row.toSeq :+ 1.0)
          case _ => Row.fromSeq(row.toSeq :+ (valueSum / numValues))
        }
      }
    }
  }

}
