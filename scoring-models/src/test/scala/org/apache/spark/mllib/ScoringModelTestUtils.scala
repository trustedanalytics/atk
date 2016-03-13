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
package org.apache.spark.mllib

import org.scalatest.Assertions._
import org.trustedanalytics.atk.scoring.interfaces.Model

import scala.util.Random

object ScoringModelTestUtils {
  // Calls model.score() with null data and verifies that we get a NullPointerException
  def nullDataTest(model: Model) = {

    // score with null data and expect a NullPointerException
    intercept[NullPointerException] {
      model.score(null)
    }
  }

  // Generates data with one less than the number of columns specified, then scores the model,
  // and expects to get an IllegalArgumentException
  def tooFewDataColumnsTest(model: Model, numColumns: Int) = {
    assert(numColumns > 1)
    // score model and expect an IllegalArgumentException
    intercept[IllegalArgumentException] {
      model.score(getRandomIntegerArray(numColumns - 1))
    }
  }

  // Generates data with one more than the number of columns specified, then scores the model,
  // and expects to get an IllegalArgumentException
  def tooManyDataColumnsTest(model: Model, numColumns: Int) = {
    assert(numColumns > 0)
    // score model and expect an IllegalArgumentException
    intercept[IllegalArgumentException] {
      model.score(getRandomIntegerArray(numColumns + 1))
    }
  }

  // Generates data with non-numerical values and then scores the model.  Expects to get
  // a NumberFormatException
  def invalidDataTest(model: Model, numColumns: Int) = {
    assert(numColumns > 0)

    var data = Array[Any]()

    // generate data by getting integers and then adding on an "a"
    val row1 = getRandomIntegerArray(numColumns)
    for (i <- row1.indices) {
      row1(i) += "a"
    }
    intercept[NumberFormatException] {
      model.score(row1)
    }
  }

  // Generates data with float data for the specified number of column/rows and then
  // scores the model.  Verifies that the result returned has the expected length.
  def successfulModelScoringFloatTest(model: Model, numColumns: Int) = {
    // score model and check result length
    val score = model.score(getRandomFloatArray(numColumns))
    assert(score.length == numColumns + 1)
  }

  // Generates data with integer data for the specified number of column/rows and then
  // scores the model.  Verifies that the result returned has the expected length.
  def successfulModelScoringIntegerTest(model: Model, numColumns: Int) = {
    // score model and check result length
    val score = model.score(getRandomIntegerArray(numColumns))
    assert(score.length == numColumns + 1)
  }

  //Verifies that the input API on the model returns the correct number of observation columns that the
  //model was trained on
  def successfulInputTest(model: Model, numObsCols: Int) = {
    val input = model.input()
    assert(input.length == numObsCols)
  }

  //Verifies that the length of the array returned by the output API on the model is one more than the
  // number of observation columns that the model was trained in
  def successfulOutputTest(model: Model, numObsCols: Int) = {
    val output = model.output()
    assert(output.length == numObsCols + 1)
  }

  // Helper function to return an array filled with random string floats
  def getRandomFloatArray(length: Int): Array[Any] = {
    val row = new Array[String](length)

    for (i <- 0 until row.length) {
      row(i) = Random.nextFloat().toString()
    }
    row.asInstanceOf[Array[Any]]
  }

  // Helper function to return an array filled with random string integers
  def getRandomIntegerArray(length: Int): Array[Any] = {
    val row = new Array[String](length)

    for (i <- 0 until row.length) {
      row(i) = Random.nextInt().toString()
    }
    row.asInstanceOf[Array[Any]]
  }

}
