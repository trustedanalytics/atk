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

package org.trustedanalytics.atk.engine.frame.plugins.bincolumn

import org.trustedanalytics.atk.domain.command.CommandDoc
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema, Schema }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.SparkContext._

@PluginDoc(oneLine = "Compute the histogram for a column in a frame.",
  extended = """Compute the histogram of the data in a column.
The returned value is a Histogram object containing 3 lists one each for:
the cutoff points of the bins, size of each bin, and density of each bin.

**Notes**

The num_bins parameter is considered to be the maximum permissible number
of bins because the data may dictate fewer bins.
With equal depth binning, for example, if the column to be binned has 10
elements with only 2 distinct values and the *num_bins* parameter is
greater than 2, then the number of actual number of bins will only be 2.
This is due to a restriction that elements with an identical value must
belong to the same bin.""",
  returns = """histogram
    A Histogram object containing the result set.
    The data returned is composed of multiple components:
cutoffs : array of float
    A list containing the edges of each bin.
hist : array of float
    A list containing count of the weighted observations found in each bin.
density : array of float
    A list containing a decimal containing the percentage of
    observations found in the total set per bin.""")
class HistogramPlugin extends SparkCommandPlugin[HistogramArgs, Histogram] {

  override def name: String = "frame/histogram"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def numberOfJobs(arguments: HistogramArgs)(implicit invocation: Invocation): Int = arguments.binType match {
    case Some("equaldepth") => 8
    case _ => 7
  }

  /**
   * Compute histogram for a column in a frame.
   * @param arguments histogram arguments, frame, column, column of weights, and number of bins
   * @param invocation current invocation
   * @return Histogram object containing three Seqs one each for, cutoff points of the bins, size of bins, and percentages of total results per bin
   */
  override def execute(arguments: HistogramArgs)(implicit invocation: Invocation): Histogram = {
    val frame: SparkFrame = arguments.frame
    val schema = frame.schema

    val columnIndex: Int = schema.columnIndex(arguments.columnName)
    val columnType = schema.columnDataType(arguments.columnName)
    require(columnType.isNumerical, s"Invalid column ${arguments.columnName} for histogram.  Expected a numerical data type, but got $columnType.")

    val weightColumnIndex: Option[Int] = arguments.weightColumnName match {
      case Some(n) =>
        val columnType = schema.columnDataType(n)
        require(columnType.isNumerical, s"Invalid column $n for bin column.  Expected a numerical data type, but got $columnType.")
        Some(schema.columnIndex(n))
      case None => None
    }

    val numBins: Int = HistogramPlugin.getNumBins(arguments.numBins, frame)

    computeHistogram(frame.rdd, columnIndex, weightColumnIndex, numBins, arguments.binType.getOrElse("equalwidth") == "equalwidth")
  }

  /**
   * compute histogram information from column in a dataFrame
   * @param dataFrame rdd containing the required information
   * @param columnIndex index of column to compute information against
   * @param weightColumnIndex optional index of a column containing the weighted value of each record. Must be numeric, will assume to equal 1 if not included
   * @param numBins number of bins to compute
   * @param equalWidth true if we are using equalwidth binning false if not
   * @return a new RDD containing the inclusive start, exclusive end, size and density of each bin.
   */
  private[bincolumn] def computeHistogram(dataFrame: RDD[Row], columnIndex: Int, weightColumnIndex: Option[Int], numBins: Int, equalWidth: Boolean = true): Histogram = {
    val binnedResults = if (equalWidth)
      DiscretizationFunctions.binEqualWidth(columnIndex, numBins, dataFrame)
    else
      DiscretizationFunctions.binEqualDepth(columnIndex, numBins, weightColumnIndex, dataFrame)

    //get the size of each observation in the rdd. if it is negative do not count the observation
    //todo: warn user if a negative weight appears
    val pairedRDD: RDD[(Int, Double)] = binnedResults.rdd.map(row => (DataTypes.toInt(row.toSeq.last),
      weightColumnIndex match {
        case Some(i) => math.max(DataTypes.toDouble(row(i)), 0.0)
        case None => HistogramPlugin.UNWEIGHTED_OBSERVATION_SIZE
      })).reduceByKey(_ + _)

    val filledBins = pairedRDD.collect()
    val emptyBins = (0 to binnedResults.cutoffs.length - 2).map(i => (i, 0.0))
    //reduce by key and return either 0 or the value from filledBins
    val bins = (filledBins ++ emptyBins).groupBy(_._1).map {
      case (key, values) => (key, values.map(_._2).max)
    }.toList

    //sort by key return values
    val histSizes: Seq[Double] = bins.sortBy(_._1).map(_._2)

    val totalSize: Double = histSizes.sum
    val frequencies: Seq[Double] = histSizes.map(size => size / totalSize)

    new Histogram(binnedResults.cutoffs, histSizes, frequencies)
  }

}

object HistogramPlugin {
  val MAX_COMPUTED_NUMBER_OF_BINS: Int = 1000
  val UNWEIGHTED_OBSERVATION_SIZE: Double = 1.0

  def getNumBins(numBins: Option[Int], frame: SparkFrame): Int = {
    numBins match {
      case Some(n) => n
      case None =>
        math.min(math.floor(math.sqrt(frame.rowCount match {
          case Some(r) => r
          case None => frame.rdd.count()
        })), HistogramPlugin.MAX_COMPUTED_NUMBER_OF_BINS).toInt
    }
  }
}
