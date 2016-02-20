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

package org.trustedanalytics.atk.engine.frame.plugins

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.frame.plugins.cumulativedist.CumulativeDistFunctions

/**
 * Functions for computing various types of exponential functions
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object ExponentialFunctions extends Serializable {

  /**
   * Calculates sum (exp (x))
   * @param frameRdd input frame
   * @param columnName column name
   * @return the sum as double
   */
  def columnSum(frameRdd: FrameRdd, columnName: String): Double = {

    columnSum(CumulativeDistFunctions.pairedRdd(frameRdd, columnName).values).sum
  }

  /**
   * Calculates sum (exp(a*x)), where a is a constant multiplier
   * @param frameRdd input frame
   * @param columnName column name
   * @param exponentMultiplier common multiplier for the exponent
   * @return the sum as double
   */
  def columnSum(frameRdd: FrameRdd, columnName: String, exponentMultiplier: Double): Double = {

    columnSum(CumulativeDistFunctions.pairedRdd(frameRdd, columnName).values, exponentMultiplier).sum
  }

  /**
   * Calculates sum (x * exp(a*x)), where a is a constant multiplier
   * @param frameRdd input frame
   * @param columnName column name
   * @param exponentMultiplier common multiplier for the exponent
   * @return the sum as double
   */
  def columnSumWithMultiplier(frameRdd: FrameRdd, columnName: String, exponentMultiplier: Double): Double = {

    columnSumWithMultiplier(CumulativeDistFunctions.pairedRdd(frameRdd, columnName).values, exponentMultiplier).sum
  }

  /**
   * Calculates sum ((x * x) * exp(a*x)), where a is a constant multiplier
   * @param frameRdd input frame
   * @param columnName column name
   * @param exponentMultiplier common multiplier for the exponent
   * @return the sum as double
   */
  def columnSumWithSquareMultiplier(frameRdd: FrameRdd, columnName: String, exponentMultiplier: Double): Double = {

    columnSumWithSquaredMultiplier(CumulativeDistFunctions.pairedRdd(frameRdd, columnName).values, exponentMultiplier).sum
  }

  /**
   * Calculates sum (exp (x))
   * @param initial input frame
   * @return the sum as double
   */
  def columnSum(initial: RDD[(Double)]): RDD[Double] = {

    initial.map(x => Math.exp(x))
  }

  /**
   * Calculates sum (exp(a*x)), where a is a constant multiplier
   * @param initial input frame
   * @param exponentMultiplier common multiplier for the exponent
   * @return the sum as double
   */
  def columnSum(initial: RDD[(Double)], exponentMultiplier: Double): RDD[Double] = {

    initial.map(x => Math.exp(x * exponentMultiplier))
  }

  /**
   * Calculates sum (x * exp(a*x)), where a is a constant multiplier
   * @param initial input frame
   * @param exponentMultiplier common multiplier for the exponent
   * @return the sum as double
   */
  def columnSumWithMultiplier(initial: RDD[(Double)], exponentMultiplier: Double): RDD[Double] = {

    initial.map(x => x * Math.exp(x * exponentMultiplier))
  }

  /**
   * Calculates sum ((x * x) * exp(a*x)), where a is a constant multiplier
   * @param initial input frame
   * @param exponentMultiplier common multiplier for the exponent
   * @return the sum as double
   */
  def columnSumWithSquaredMultiplier(initial: RDD[(Double)], exponentMultiplier: Double): RDD[Double] = {

    initial.map(x => Math.pow(x, 2) * Math.exp(x * exponentMultiplier))
  }
}
