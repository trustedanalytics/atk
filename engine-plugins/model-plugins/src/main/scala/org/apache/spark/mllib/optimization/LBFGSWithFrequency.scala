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


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.optimization

import breeze.linalg.{ DenseMatrix => BDM, DenseVector => BDV }
import breeze.optimize.{ CachedDiffFunction, LBFGS => BreezeLBFGS }
import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.evaluation.{ ApproximateHessianMatrix, HessianMatrix }
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * :: DeveloperApi ::
 * Class used to solve an optimization problem using Limited-memory BFGS.
 * Reference: [[http://en.wikipedia.org/wiki/Limited-memory_BFGS]]
 *
 * Extension of MlLib's L-BFGS that supports a frequency column.
 *
 * @param gradient Gradient function to be used.
 * @param updater Updater to be used to update weights after every iteration.
 */
@DeveloperApi
class LBFGSWithFrequency(private var gradient: GradientWithFrequency, private var updater: Updater)
    extends OptimizerWithFrequency with Logging with HessianMatrix {

  private var numCorrections = 10
  private var convergenceTol = 1E-4
  private var maxNumIterations = 100
  private var regParam = 0.0
  private var computeHessian = false
  private var hessianMatrix: Option[BDM[Double]] = None

  /**
   * Set the number of corrections used in the LBFGS update. Default 10.
   * Values of numCorrections less than 3 are not recommended; large values
   * of numCorrections will result in excessive computing time.
   * 3 < numCorrections < 10 is recommended.
   * Restriction: numCorrections > 0
   */
  def setNumCorrections(corrections: Int): this.type = {
    assert(corrections > 0)
    this.numCorrections = corrections
    this
  }

  /**
   * Set the convergence tolerance of iterations for L-BFGS. Default 1E-4.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * This value must be nonnegative. Lower convergence values are less tolerant
   * and therefore generally cause more iterations to be run.
   */
  def setConvergenceTol(tolerance: Double): this.type = {
    this.convergenceTol = tolerance
    this
  }

  /**
   * Set the maximal number of iterations for L-BFGS. Default 100.
   * @deprecated use [[LBFGSWithFrequency#setNumIterations]] instead
   */
  @deprecated("use setNumIterations instead", "1.1.0")
  def setMaxNumIterations(iters: Int): this.type = {
    this.setNumIterations(iters)
  }

  /**
   * Set the maximal number of iterations for L-BFGS. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.maxNumIterations = iters
    this
  }

  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for L-BFGS.
   */
  def setGradient(gradient: GradientWithFrequency): this.type = {
    this.gradient = gradient
    this
  }

  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  /**
   * Set flag for computing the Hessian matrix.
   *
   * If true, a Hessian matrix for the weights is computed once the optimization completes.
   */
  def setComputeHessian(computeHessian: Boolean): this.type = {
    this.computeHessian = computeHessian
    this
  }

  /**
   * Get the approximate Hessian matrix
   */
  override def getHessianMatrix: Option[BDM[Double]] = hessianMatrix

  override def optimizeWithFrequency(data: RDD[(Double, Vector, Double)], initialWeights: Vector): Vector = {
    val (weights, _, hessian) = LBFGSWithFrequency.runLBFGS(
      data,
      gradient,
      updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeights,
      computeHessian
    )

    this.hessianMatrix = hessian
    weights
  }

}

/**
 * :: DeveloperApi ::
 * Top-level method to run L-BFGS.
 */
@DeveloperApi
object LBFGSWithFrequency extends Logging {
  /**
   * Run Limited-memory BFGS (L-BFGS) in parallel.
   * Averaging the subgradients over different partitions is performed using one standard
   * spark map-reduce in each iteration.
   *
   * @param data - Input data for L-BFGS. RDD of the set of data examples, each of
   *             the form (label, [feature values]).
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                 one single data example)
   * @param updater - Updater function to actually perform a gradient step in a given direction.
   * @param numCorrections - The number of corrections used in the L-BFGS update.
   * @param convergenceTol - The convergence tolerance of iterations for L-BFGS which is must be
   *                       nonnegative. Lower values are less tolerant and therefore generally
   *                       cause more iterations to be run.
   * @param maxNumIterations - Maximal number of iterations that L-BFGS can be run.
   * @param regParam - Regularization parameter
   * @param computeHessian If true, compute Hessian matrix for the model
   *
   * @return A tuple containing three elements. The first element is a column matrix containing
   *         weights for every feature, the second element is an array containing the loss
   *         computed for every iteration, and the third element is the approximate Hessian matrix.
   */
  def runLBFGS(
    data: RDD[(Double, Vector, Double)],
    gradient: GradientWithFrequency,
    updater: Updater,
    numCorrections: Int,
    convergenceTol: Double,
    maxNumIterations: Int,
    regParam: Double,
    initialWeights: Vector,
    computeHessian: Boolean): (Vector, Array[Double], Option[BDM[Double]]) = {

    val lossHistory = mutable.ArrayBuilder.make[Double]

    val numExamples = data.count()

    val costFun =
      new CostFunctionWithFrequency(data, gradient, updater, regParam, numExamples)

    val lbfgs = new BreezeLBFGS[BDV[Double]](maxNumIterations, numCorrections, convergenceTol)

    val states =
      lbfgs.iterations(new CachedDiffFunction(costFun), initialWeights.toBreeze.toDenseVector)

    /**
     * NOTE: lossSum and loss is computed using the weights from the previous iteration
     * and regVal is the regularization value computed in the previous iteration as well.
     */
    var state = states.next()
    while (states.hasNext) {
      lossHistory += state.value
      state = states.next()
    }
    lossHistory += state.value
    val weights = Vectors.fromBreeze(state.x)

    val lossHistoryArray = lossHistory.result()

    // Compute the approximate Hessian matrix using weights for the final iteration
    val hessianMatrix = if (computeHessian) {
      Some(ApproximateHessianMatrix.computeHessianMatrix(data, weights, gradient,
        updater, regParam, numExamples))
    }
    else None

    logInfo("LBFGS.runLBFGS finished. Last 10 losses %s".format(
      lossHistoryArray.takeRight(10).mkString(", ")))

    (weights, lossHistoryArray, hessianMatrix)
  }
}
