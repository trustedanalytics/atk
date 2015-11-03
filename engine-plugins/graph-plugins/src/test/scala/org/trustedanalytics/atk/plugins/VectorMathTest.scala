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

package org.trustedanalytics.atk.plugins

import org.scalatest.{ Matchers, FlatSpec }

/**
 * Provides simple tests for the vector math routines.
 * In particular -
 * 1. basic numerical correctness
 * 2. correct promotion of vectors of differeing lengths
 * 3. handling the null vector
 */
class VectorMathTest extends FlatSpec with Matchers {

  trait VectorTest {

    val comparisonThreshold = 0.00000001d

    val nullVector = Vector.empty[Double]

    val unitInOneDimension = Vector(1.0d)

    val xUnitInTwoDimensions = Vector(1.0d, 0.0d)
    val yUnitInTwoDimensions = Vector(0.0d, 1.0d)

    val xUnitInThreeDimensions = Vector(1.0d, 0.0d, 0.0d)
    val yUnitInThreeDimensions = Vector(0.0d, 1.0d, 0.0d)
    val zUnitInThreeDimensions = Vector(0.0d, 0.0d, 1.0d)
    val zeroInThreeDimensions = Vector(0d, 0d, 0d)

    val vector1 = Vector(0.1d, 0.2d, 0.3d, 0.4d, 0.5d)
    val vector2 = Vector(0.5d, 0.4d, 0.3d, 0.2d, 0.1d)
    val expectedProduct = Vector(0.05d, 0.08d, 0.09d, 0.08d, 0.05d)

    val expectedProductXProjection = Vector(0.05d, 0d, 0d, 0d, 0d)
    val expectedProductZProjection = Vector(0.0d, 0d, 0.09d, 0d, 0d)
  }

  "componentwise logarithm" should "return null vector for null vector" in new VectorTest {

    val test = VectorMath.componentwiseLog(nullVector)
    test shouldEqual nullVector
  }

  "componentwise logarithm" should "produce 0 at the 1 and negative infinities on the zeroes for a unit vector" in new VectorTest {

    val test = VectorMath.componentwiseLog(yUnitInThreeDimensions)
    test shouldEqual Vector(Double.NegativeInfinity, 0.0d, Double.NegativeInfinity)
  }

  "componentwise logarithm" should "take the componentwise logarithm of a vector" in new VectorTest {
    val expected = vector1.map({ case x: Double => Math.log(x) })

    val test = VectorMath.componentwiseLog(vector1)

    test shouldEqual expected
  }

  "componentwise exponentiation" should "return null vector for null vector" in new VectorTest {
    val test = VectorMath.componentwiseExponentiation(nullVector)

    test shouldEqual nullVector
  }

  "componentwise exponentiation" should "take the componentwise exponentiation of a vector" in new VectorTest {
    val expected = vector1.map({ case x: Double => Math.exp(x) })

    val test = VectorMath.componentwiseExponentiation(vector1)

    test shouldEqual expected
  }

  "sum of of two vectors" should "return null vector for sum of two null vectors" in new VectorTest {
    val test = VectorMath.sum(nullVector, nullVector)
    test shouldEqual nullVector
  }

  "sum of of two vectors" should "return the original non-null vector when a non-null vector is added to the null vector" in new VectorTest {
    val test = VectorMath.sum(nullVector, vector1)
    test shouldEqual vector1

    val otherTest = VectorMath.sum(vector1, nullVector)
    otherTest shouldEqual vector1
  }

  "sum of two vectors" should "pad shorter vector with zeroes when adding vectors of different lengths" in new VectorTest {

    val test = VectorMath.sum(xUnitInTwoDimensions, vector1)
    val expected = Vector(1.1d, 0.2d, 0.3d, 0.4d, 0.5d)
    test shouldEqual expected

    val otherTest = VectorMath.sum(xUnitInTwoDimensions, vector1)
    otherTest shouldEqual expected
  }

  "l1 norm" should "return 0 for the null vector" in new VectorTest {
    val test = VectorMath.l1Norm(nullVector)
    test shouldEqual 0d
  }

  "l1 norm" should "calculate the l1 norm of a vector" in new VectorTest {
    val test = VectorMath.l1Norm(vector1)

    test shouldEqual vector1.map(Math.abs).sum
  }

  "l1 normalization" should "return the null vector for the null vector" in new VectorTest {
    val test = VectorMath.l1Normalize(nullVector)
    test shouldEqual nullVector
  }

  "l1 normalization" should "return the same zero-norm vector when given a zero-norm vector" in new VectorTest {
    val test = VectorMath.l1Normalize(zeroInThreeDimensions)
    test shouldEqual zeroInThreeDimensions
  }

  "l1 normalization" should "return the l1 normalization of its input vector" in new VectorTest {
    val test = VectorMath.l1Normalize(vector1)
    val l1Norm = vector1.map(Math.abs).sum

    test shouldEqual vector1.map(x => x / l1Norm)
  }

  "overflow protected product" should "return the (approximate) component-wise product of two vectors" in new VectorTest {
    val test: Vector[Double] = VectorMath.overflowProtectedProduct(vector1, vector2).get

    val comparison = expectedProduct.zip(test).
      map({ case (x: Double, y: Double) => Math.abs(x - y) }).forall(x => x < comparisonThreshold)

    comparison shouldEqual true
  }

  "overlow protected product" should "return None when given an empty list" in new VectorTest {
    val test = VectorMath.overflowProtectedProduct(List.empty[Vector[Double]])
    test shouldEqual None
  }

  "overflow protected product" should "return the (approximate) component-wise product of three vectors" in new VectorTest {
    val test1: Vector[Double] = VectorMath.overflowProtectedProduct(List(vector1, vector2, xUnitInThreeDimensions)).get

    val comparison1 = expectedProductXProjection.zip(test1).
      map({ case (x: Double, y: Double) => Math.abs(x - y) }).forall(x => x < comparisonThreshold)

    comparison1 shouldEqual true

    val test2: Vector[Double] = VectorMath.overflowProtectedProduct(List(vector1, vector2, zUnitInThreeDimensions)).get

    val comparison2 = expectedProductZProjection.zip(test2).
      map({ case (x: Double, y: Double) => Math.abs(x - y) }).forall(x => x < comparisonThreshold)

    comparison2 shouldEqual true
  }

}
