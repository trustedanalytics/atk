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
package org.trustedanalytics.atk.engine.frame.plugins

import org.trustedanalytics.atk.domain.DoubleValue
import org.trustedanalytics.atk.domain.frame.EntropyArgs
import org.trustedanalytics.atk.domain.schema.Column
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.frame.plugins.statistics.descriptives.ColumnStatistics
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.engine.frame.plugins.statistics.NumericValidationUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.util.Try

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Calculate Shannon entropy of a column.
 *
 * Entropy is a measure of the uncertainty in a random variable.
 */
@PluginDoc(oneLine = "Calculate the Shannon entropy of a column.",
  extended = """The data column is weighted via the weights column.
All data elements of weight <= 0 are excluded from the calculation, as are
all data elements whose weight is NaN or infinite.
If there are no data elements with a finite weight greater than 0,
the entropy is zero.""",
  returns = "Entropy.")
class EntropyPlugin extends SparkCommandPlugin[EntropyArgs, DoubleValue] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/entropy"

  /**
   * Calculate Shannon entropy of a column.
   *
   * Entropy is a measure of the uncertainty in a random variable.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: EntropyArgs)(implicit invocation: Invocation): DoubleValue = {
    // validate arguments
    val frame: SparkFrame = arguments.frame
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)

    // run the operation and return results
    val weightsColumnOption = frame.schema.column(arguments.weightsColumn)
    val entropy = EntropyRddFunctions.shannonEntropy(frame.rdd, columnIndex, weightsColumnOption)
    DoubleValue(entropy)
  }
}

/**
 * Functions for computing entropy.
 *
 * Entropy is a measure of the uncertainty in a random variable.
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private object EntropyRddFunctions extends Serializable {

  /**
   * Calculate the Shannon entropy for specified column in data frame.
   *
   * @param frameRdd RDD for data frame
   * @param dataColumnIndex Index of data column
   * @param weightsColumnOption Option for column providing the weights. Must be numerical data.
   * @return Weighted shannon entropy (using natural log)
   */
  def shannonEntropy(frameRdd: RDD[Row],
                     dataColumnIndex: Int,
                     weightsColumnOption: Option[Column] = None): Double = {
    require(dataColumnIndex >= 0, "column index must be greater than or equal to zero")

    val dataWeightPairs =
      ColumnStatistics.getDataWeightPairs(dataColumnIndex, weightsColumnOption, frameRdd)
        .filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val distinctCountRDD = dataWeightPairs.reduceByKey(_ + _).map({ case (value, count) => count })

    // sum() throws an exception if RDD is empty so catching it and returning zero
    val totalCount = Try(distinctCountRDD.sum()).getOrElse(0d)

    val entropy = if (totalCount > 0) {
      val distinctProbabilities = distinctCountRDD.map(count => count / totalCount)
      -distinctProbabilities.map(probability => if (probability > 0) probability * math.log(probability) else 0).sum()
    }
    else 0d

    entropy
  }
}
