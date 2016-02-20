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

package org.apache.spark.mllib.regression

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.util.MLUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ Logging, SparkException }

/**
 * :: DeveloperApi ::
 * GeneralizedLinearModel (GLM) represents a model trained using
 * GeneralizedLinearAlgorithm. GLMs consist of a weight vector and
 * an intercept.
 *
 * Extension of MlLib's generalized linear model that supports a frequency column.
 * The frequency column contains the frequency of occurrence of each observation.
 * @see org.apache.spark.mllib.regression.GeneralizedLinearModel
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
@DeveloperApi
abstract class GeneralizedLinearModelWithFrequency(val weights: Vector, val intercept: Double)
    extends Serializable {

  /**
   * Predict the result given a data point and the weights learned.
   *
   * @param dataMatrix Row vector containing the features for this data point
   * @param weightMatrix Column vector containing the weights of the model
   * @param intercept Intercept of the model.
   */
  protected def predictPoint(dataMatrix: Vector, weightMatrix: Vector, intercept: Double): Double

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   */
  def predict(testData: RDD[Vector]): RDD[Double] = {
    // A small optimization to avoid serializing the entire model. Only the weightsMatrix
    // and intercept is needed.
    val localWeights = weights
    val bcWeights = testData.context.broadcast(localWeights)
    val localIntercept = intercept
    testData.mapPartitions { iter =>
      val w = bcWeights.value
      iter.map(v => predictPoint(v, w, localIntercept))
    }
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return Double prediction from the trained model
   */
  def predict(testData: Vector): Double = {
    predictPoint(testData, weights, intercept)
  }

  /**
   * Print a summary of the model.
   */
  override def toString: String = {
    s"${this.getClass.getName}: intercept = $intercept, numFeatures = ${weights.size}"
  }
}

/**
 * :: DeveloperApi ::
 * GeneralizedLinearAlgorithm implements methods to train a Generalized Linear Model (GLM).
 * This class should be extended with an Optimizer to create a new GLM.
 */
@DeveloperApi
abstract class GeneralizedLinearAlgorithmWithFrequency[M <: GeneralizedLinearModelWithFrequency]
    extends Logging with Serializable {

  protected val validators: Seq[RDD[LabeledPointWithFrequency] => Boolean] = List()

  /** The optimizer to solve the problem. */
  def optimizer: OptimizerWithFrequency

  /** Whether to add intercept (default: false). */
  protected var addIntercept: Boolean = false

  protected var validateData: Boolean = true

  /**
   * In `GeneralizedLinearModel`, only single linear predictor is allowed for both weights
   * and intercept. However, for multinomial logistic regression, with K possible outcomes,
   * we are training K-1 independent binary logistic regression models which requires K-1 sets
   * of linear predictor.
   *
   * As a result, the workaround here is if more than two sets of linear predictors are needed,
   * we construct bigger `weights` vector which can hold both weights and intercepts.
   * If the intercepts are added, the dimension of `weights` will be
   * (numOfLinearPredictor) * (numFeatures + 1) . If the intercepts are not added,
   * the dimension of `weights` will be (numOfLinearPredictor) * numFeatures.
   *
   * Thus, the intercepts will be encapsulated into weights, and we leave the value of intercept
   * in GeneralizedLinearModel as zero.
   */
  protected var numOfLinearPredictor: Int = 1

  /**
   * Whether to perform feature scaling before model training to reduce the condition numbers
   * which can significantly help the optimizer converging faster. The scaling correction will be
   * translated back to resulting model weights, so it's transparent to users.
   * Note: This technique is used in both libsvm and glmnet packages. Default false.
   */
  private var useFeatureScaling = false

  /**
   * The dimension of training features.
   */
  def getNumFeatures: Int = this.numFeatures

  /**
   * The dimension of training features.
   */
  protected var numFeatures: Int = -1

  /**
   * Set if the algorithm should use feature scaling to improve the convergence during optimization.
   */
  def setFeatureScaling(useFeatureScaling: Boolean): this.type = {
    this.useFeatureScaling = useFeatureScaling
    this
  }

  /**
   * Create a model given the weights, and intercept
   */
  protected def createModel(weights: Vector,
                            intercept: Double): M

  /**
   * Get if the algorithm uses addIntercept
   */
  def isAddIntercept: Boolean = this.addIntercept

  /**
   * Set if the algorithm should add an intercept. Default false.
   * We set the default to false because adding the intercept will cause memory allocation.
   */
  def setIntercept(addIntercept: Boolean): this.type = {
    this.addIntercept = addIntercept
    this
  }

  /**
   * Set if the algorithm should validate data before training. Default true.
   */
  def setValidateData(validateData: Boolean): this.type = {
    this.validateData = validateData
    this
  }

  /**
   * Run the algorithm with the configured parameters on an input
   * RDD of LabeledPoint entries.
   */
  def run(input: RDD[LabeledPointWithFrequency]): M = {
    if (numFeatures < 0) {
      numFeatures = input.map(_.features.size).first()
    }

    /**
     * When `numOfLinearPredictor > 1`, the intercepts are encapsulated into weights,
     * so the `weights` will include the intercepts. When `numOfLinearPredictor == 1`,
     * the intercept will be stored as separated value in `GeneralizedLinearModel`.
     * This will result in different behaviors since when `numOfLinearPredictor == 1`,
     * users have no way to set the initial intercept, while in the other case, users
     * can set the intercepts as part of weights.
     *
     * TODO: See if we can deprecate `intercept` in `GeneralizedLinearModel`, and always
     * have the intercept as part of weights to have consistent design.
     */
    val initialWeights = {
      if (numOfLinearPredictor == 1) {
        Vectors.zeros(numFeatures)
      }
      else if (addIntercept) {
        Vectors.zeros((numFeatures + 1) * numOfLinearPredictor)
      }
      else {
        Vectors.zeros(numFeatures * numOfLinearPredictor)
      }
    }
    run(input, initialWeights)
  }

  /**
   * Run the algorithm with the configured parameters on an input RDD
   * of LabeledPoint entries starting from the initial weights provided.
   */
  def run(input: RDD[LabeledPointWithFrequency], initialWeights: Vector): M = {

    if (numFeatures < 0) {
      numFeatures = input.map(_.features.size).first()
    }

    if (input.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    // Check the data properties before running the optimizer
    if (validateData && !validators.forall(func => func(input))) {
      throw new SparkException("Input validation failed.")
    }

    /**
     * Scaling columns to unit variance as a heuristic to reduce the condition number:
     *
     * During the optimization process, the convergence (rate) depends on the condition number of
     * the training dataset. Scaling the variables often reduces this condition number
     * heuristically, thus improving the convergence rate. Without reducing the condition number,
     * some training datasets mixing the columns with different scales may not be able to converge.
     *
     * GLMNET and LIBSVM packages perform the scaling to reduce the condition number, and return
     * the weights in the original scale.
     * See page 9 in http://cran.r-project.org/web/packages/glmnet/glmnet.pdf
     *
     * Here, if useFeatureScaling is enabled, we will standardize the training features by dividing
     * the variance of each column (without subtracting the mean), and train the model in the
     * scaled space. Then we transform the coefficients from the scaled space to the original scale
     * as GLMNET and LIBSVM do.
     *
     * Currently, it's only enabled in LogisticRegressionWithLBFGS
     */
    val scaler = if (useFeatureScaling) {
      new StandardScaler(withStd = true, withMean = false).fit(input.map(_.features))
    }
    else {
      null
    }

    // Prepend an extra variable consisting of all 1.0's for the intercept.
    // TODO: Apply feature scaling to the weight vector instead of input data.
    val data =
      if (addIntercept) {
        if (useFeatureScaling) {
          input.map(lp => (lp.label, appendBias(scaler.transform(lp.features)), lp.frequency)).cache()
        }
        else {
          input.map(lp => (lp.label, appendBias(lp.features), lp.frequency)).cache()
        }
      }
      else {
        if (useFeatureScaling) {
          input.map(lp => (lp.label, scaler.transform(lp.features), lp.frequency)).cache()
        }
        else {
          input.map(lp => (lp.label, lp.features, lp.frequency))
        }
      }

    /**
     * TODO: For better convergence, in logistic regression, the intercepts should be computed
     * from the prior probability distribution of the outcomes; for linear regression,
     * the intercept should be set as the average of response.
     */
    val initialWeightsWithIntercept = if (addIntercept && numOfLinearPredictor == 1) {
      appendBias(initialWeights)
    }
    else {
      /** If `numOfLinearPredictor > 1`, initialWeights already contains intercepts. */
      initialWeights
    }

    val weightsWithIntercept = optimizer.optimizeWithFrequency(data, initialWeightsWithIntercept)

    val intercept = if (addIntercept && numOfLinearPredictor == 1) {
      weightsWithIntercept(weightsWithIntercept.size - 1)
    }
    else {
      0.0
    }

    var weights = if (addIntercept && numOfLinearPredictor == 1) {
      Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1))
    }
    else {
      weightsWithIntercept
    }

    /**
     * The weights and intercept are trained in the scaled space; we're converting them back to
     * the original scale.
     *
     * Math shows that if we only perform standardization without subtracting means, the intercept
     * will not be changed. w_i = w_i' / v_i where w_i' is the coefficient in the scaled space, w_i
     * is the coefficient in the original space, and v_i is the variance of the column i.
     */
    if (useFeatureScaling) {
      if (numOfLinearPredictor == 1) {
        weights = scaler.transform(weights)
      }
      else {
        /**
         * For `numOfLinearPredictor > 1`, we have to transform the weights back to the original
         * scale for each set of linear predictor. Note that the intercepts have to be explicitly
         * excluded when `addIntercept == true` since the intercepts are part of weights now.
         */
        var i = 0
        val n = weights.size / numOfLinearPredictor
        val weightsArray = weights.toArray
        while (i < numOfLinearPredictor) {
          val start = i * n
          val end = (i + 1) * n - {
            if (addIntercept) 1 else 0
          }

          val partialWeightsArray = scaler.transform(
            Vectors.dense(weightsArray.slice(start, end))).toArray

          System.arraycopy(partialWeightsArray, 0, weightsArray, start, partialWeightsArray.size)
          i += 1
        }
        weights = Vectors.dense(weightsArray)
      }
    }

    // Warn at the end of the run as well, for increased visibility.
    if (input.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    createModel(weights, intercept)
  }
}
