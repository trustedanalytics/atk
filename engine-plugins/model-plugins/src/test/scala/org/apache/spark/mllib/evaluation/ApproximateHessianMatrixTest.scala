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
package org.apache.spark.mllib.evaluation

import breeze.linalg.{ sum, DenseMatrix, DenseVector }
import breeze.numerics._
import breeze.optimize.DiffFunction
import breeze.util.DoubleImplicits
import org.scalatest.{ FlatSpec, Matchers }

class ApproximateHessianMatrixTest extends FlatSpec with Matchers with DoubleImplicits {

  "ApproximateHessian" should "compute hessian matrix of rosenbrook function" in {
    val inputVector = DenseVector(-1.2d, 1d)
    val expectedHessian = DenseMatrix((1330d, 480d), (480d, 200d))

    val diffFunction = new DiffFunction[DenseVector[Double]] {
      def calculate(x: DenseVector[Double]) = {
        val value = 100 * Math.pow(x(1) - x(0) * x(0), 2) + Math.pow(1 - x(0), 2)
        val grad = DenseVector(
          -400 * x(0) * (x(1) - x(0) * x(0)) - 2 * (1 - x(0)),
          200 * (x(1) - x(0) * x(0))
        )
        (value, grad)
      }
    }

    val hessian = ApproximateHessianMatrix(diffFunction, inputVector).calculate()

    assert(hessian.size === expectedHessian.size)
    for (i <- 0 until expectedHessian.rows; j <- 0 until expectedHessian.cols) {
      assert(hessian(i, j).closeTo(expectedHessian(i, j)))
    }
  }

  "ApproximateHessian" should "compute hessian matrix of exponential function" in {
    val inputVector = DenseVector(3.56, -1.09, -0.31, 1.12, -1.52)
    val expectedHessian = DenseMatrix((35.1632d, 0d, 0d, 0d, 0d),
      (0d, 0.3362165d, 0d, 0d, 0d),
      (0d, 0d, 0.733447d, 0d, 0d),
      (0d, 0d, 0d, 3.064854d, 0d),
      (0d, 0d, 0d, 0d, 0.2187119d))

    val diffFunction = new DiffFunction[DenseVector[Double]] {
      def calculate(x: DenseVector[Double]) = {
        val n = x.length
        val value = sum(exp(x) - x) / n
        val grad = exp(x) - 1d
        (value, grad)
      }
    }

    val hessian = ApproximateHessianMatrix(diffFunction, inputVector).calculate()

    assert(hessian.size === expectedHessian.size)
    for (i <- 0 until expectedHessian.rows; j <- 0 until expectedHessian.cols) {
      assert(hessian(i, j).closeTo(expectedHessian(i, j)))
    }
  }

  "ApproximateHessian" should "return an empty hessian matrix" in {
    val x = DenseVector[Double]()

    val diffFunction = new DiffFunction[DenseVector[Double]] {
      def calculate(x: DenseVector[Double]) = {
        val n = x.length
        val value = sum(exp(x) - x) / n
        val grad = exp(x) - 1d
        (value, grad)
      }
    }

    val hessian = ApproximateHessianMatrix(diffFunction, x).calculate()

    assert(hessian.size === 0)
  }

}
