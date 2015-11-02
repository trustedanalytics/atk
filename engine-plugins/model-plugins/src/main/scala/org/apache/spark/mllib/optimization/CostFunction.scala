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

package org.apache.spark.mllib.optimization

import breeze.linalg.{ DenseVector => BDV }
import breeze.optimize.DiffFunction
import org.apache.spark.mllib.linalg.BLAS.axpy
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD

/**
 * CostFunction implements Breeze's DiffFunction[T], which returns the loss and gradient
 * at a particular point (weights). It's used in Breeze's convex optimization routines.
 *
 * This is a copy of the private CostFun class in MLlib's LBFGS class
 * @see org.apache.spark.mllib.optimization.LBFGS
 */
class CostFunctionWithFrequency(
    data: RDD[(Double, Vector, Double)],
    gradient: GradientWithFrequency,
    updater: Updater,
    regParam: Double,
    numExamples: Long) extends DiffFunction[BDV[Double]] {

  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    // Have a local copy to avoid the serialization of CostFun object which is not serializable.
    val w = Vectors.fromBreeze(weights)
    val n = w.size
    val bcW = data.context.broadcast(w)
    val localGradient = gradient

    val (gradientSum, lossSum) = data.treeAggregate((Vectors.zeros(n), 0.0))(
      seqOp = (c, v) => (c, v) match {
        case ((grad, loss), (label, features, frequency)) =>
          val l = localGradient.compute(
            features, label, frequency, bcW.value, grad)
          (grad, loss + l)
      },
      combOp = (c1, c2) => (c1, c2) match {
        case ((grad1, loss1), (grad2, loss2)) =>
          axpy(1.0, grad2, grad1)
          (grad1, loss1 + loss2)
      })

    /**
     * regVal is sum of weight squares if it's L2 updater;
     * for other updater, the same logic is followed.
     */
    val regVal = updater.compute(w, Vectors.zeros(n), 0, 1, regParam)._2

    val loss = lossSum / numExamples + regVal
    /**
     * It will return the gradient part of regularization using updater.
     *
     * Given the input parameters, the updater basically does the following,
     *
     * w' = w - thisIterStepSize * (gradient + regGradient(w))
     * Note that regGradient is function of w
     *
     * If we set gradient = 0, thisIterStepSize = 1, then
     *
     * regGradient(w) = w - w'
     *
     * TODO: We need to clean it up by separating the logic of regularization out
     * from updater to regularizer.
     */
    // The following gradientTotal is actually the regularization part of gradient.
    // Will add the gradientSum computed from the data with weights in the next step.
    val gradientTotal = w.copy
    axpy(-1.0, updater.compute(w, Vectors.zeros(n), 1, 1, regParam)._1, gradientTotal)

    // gradientTotal = gradientSum / numExamples + gradientTotal
    axpy(1.0 / numExamples, gradientSum, gradientTotal)

    (loss, gradientTotal.toBreeze.asInstanceOf[BDV[Double]])
  }
}
