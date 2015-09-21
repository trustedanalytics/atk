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

package org.trustedanalytics.atk.scoring.models

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.Matchers
import org.trustedanalytics.atk.scoring.interfaces.Model
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec
import scala.util.Random

class ScoringModelTest extends TestingSparkContextFlatSpec with ScalaFutures with Matchers  {
  val scoreTimeoutSeconds = 10   // timeout length for calling model score()
  val scoreIntervalMillis = 100  // interval to sleep between queries to check if scoring has completed

  // Calls model.score() with null data and verifies that we get a NullPointerException
  def nullDataTest (model: Model) = {
    // score with null data
    val score = model.score(null)

    // wait for scoring to complete, and we expect a NullPointerException
    whenReady(score.failed, timeout(Span(scoreTimeoutSeconds, Seconds)), interval(Span(scoreIntervalMillis, Millis))) { e =>
      e shouldBe a [NullPointerException]
    }
  }

  // Generates data with one less than the number of columns specified, then scores the model,
  // and expects to get an IllegalArgumentException
  def tooFewDataColumnsTest (model: Model, numColumns: Int, numRows: Int) = {
    assert(numColumns > 1)

    // generate data
    var data = Seq[Array[String]]()
    for (i <- 1 until numRows) {
      data = data :+ getRandomIntegerArray(numColumns-1)
    }

    // score model
    val score = model.score(data)

    // wait for scoring to complete, and we expect an IllegalArgumentException
    whenReady(score.failed, timeout(Span(scoreTimeoutSeconds, Seconds)), interval(Span(scoreIntervalMillis, Millis))) { e =>
      e shouldBe a [IllegalArgumentException]
    }
  }

  // Generates data with one more than the number of columns specified, then scores the model,
  // and expects to get an IllegalArgumentException
  def tooManyDataColumnsTest (model: Model, numColumns: Int, numRows: Int) = {
    assert(numColumns > 0)

    // generate data
    var data = Seq[Array[String]]()
    for (i <- 1 to numRows) {
      data = data :+ getRandomIntegerArray(numColumns+1)
    }

    val score = model.score(data)

    // wait for scoring to complete, and we expect an IllegalArgumentException
    whenReady(score.failed, timeout(Span(scoreTimeoutSeconds, Seconds)), interval(Span(scoreIntervalMillis, Millis))) { e =>
      e shouldBe a [IllegalArgumentException]
    }
  }

  // Generates data with non-numerical values and then scores the model.  Expects to get
  // a NumberFormatException
  def invalidDataTest (model: Model, numColumns: Int) = {
    assert(numColumns > 0)

    // generate data by getting integers and then adding on an "a"
    var row = getRandomIntegerArray(numColumns)
    for (i <- row.indices) {
      row(i) += "a"
    }

    val score = model.score(Seq(row))

    // wait for scoring to complete, and we expect an NumberFormatException
    whenReady(score.failed, timeout(Span(scoreTimeoutSeconds, Seconds)), interval(Span(scoreIntervalMillis, Millis))) { e =>
      e shouldBe a [NumberFormatException]
    }
  }

  // Generates data with float data for the specified number of column/rows and then
  // scores the model.  Verifies that the result returned has the expected length.
  def successfulModelScoringFloatTest (model: Model, numColumns: Int, numRows: Int) = {
    var data = Seq[Array[String]]()

    // generate data with float values
    for (i <- 1 to numRows) {
      data = data :+ getRandomFloatArray(numColumns)
    }

    val score = model.score(data)

    // wait for scoring to complete and check the result
    whenReady(score, timeout(Span(scoreTimeoutSeconds, Seconds)), interval(Span(scoreIntervalMillis, Millis))) { result =>
      assert(result.length == numRows)
    }
  }

  // Generates data with integer data for the specified number of column/rows and then
  // scores the model.  Verifies that the result returned has the expected length.
  def successfulModelScoringIntegerTest (model: Model, numColumns: Int, numRows: Int) = {
    var data = Seq[Array[String]]()

    // generate data with integer values
    for (i <- 1 to numRows) {
      data = data :+ getRandomIntegerArray(numColumns)
    }

    val score = model.score(data)

    // wait for scoring to complete and check the result
    whenReady(score, timeout(Span(scoreTimeoutSeconds, Seconds)), interval(Span(scoreIntervalMillis, Millis))) { result =>
      assert(result.length == numRows)
    }
  }

  // Helper function to return an array filled with random string floats
  def getRandomFloatArray (length: Int): Array[String] = {
    val row = new Array[String](length)

    for (i <- 0 until row.length) {
      row(i) = Random.nextFloat().toString()
    }
    row
  }

  // Helper function to return an array filled with random string integers
  def getRandomIntegerArray (length: Int): Array[String] = {
    val row = new Array[String](length)

    for (i <- 0 until row.length) {
      row(i) = Random.nextInt().toString()
    }
    row
  }

}
