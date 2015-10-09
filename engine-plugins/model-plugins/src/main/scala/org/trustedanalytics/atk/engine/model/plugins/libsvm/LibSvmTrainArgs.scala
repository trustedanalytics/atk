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

package org.trustedanalytics.atk.engine.model.plugins.libsvm

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for training a lib svm model with the provided dataset and params.
 */
case class LibSvmTrainArgs(model: ModelReference,
                           @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                           @ArgDoc("""Column name containing the label for each
observation.""") labelColumn: String,
                           @ArgDoc("""Column(s) containing the
observations.""") observationColumns: List[String],
                           @ArgDoc("""Set type of SVM.
Default is one-class SVM.

|   0 -- C-SVC
|   1 -- nu-SVC
|   2 -- one-class SVM
|   3 -- epsilon-SVR
|   4 -- nu-SVR""") svmType: Int = 2,
                           @ArgDoc("""Specifies the kernel type to be used in the algorithm.
Default is RBF.

|   0 -- linear: u\'\*v
|   1 -- polynomial: (gamma*u\'\*v + coef0)^degree
|   2 -- radial basis function: exp(-gamma*|u-v|^2)
|   3 -- sigmoid: tanh(gamma*u\'\*v + coef0)""") kernelType: Int = 2,
                           @ArgDoc("""Default is (Array[Int](0))""") weightLabel: Option[Array[Int]] = None,
                           @ArgDoc("""Default is (Array[Double](0.0))""") weight: Option[Array[Double]] = None,
                           @ArgDoc("""Set tolerance of termination criterion""") epsilon: Double = 0.001,
                           @ArgDoc("""Degree of the polynomial kernel function ('poly').
Ignored by all other kernels.""") degree: Int = 3,
                           @ArgDoc("""Kernel coefficient for 'rbf', 'poly' and 'sigmoid'.
Default is 1/n_features.""") gamma: Option[Double] = None,
                           @ArgDoc("""Independent term in kernel function.
It is only significant in 'poly' and 'sigmoid'.""") coef: Double = 0,
                           @ArgDoc("""Set the parameter nu of nu-SVC, one-class SVM,
and nu-SVR.""") nu: Double = 0.5,
                           @ArgDoc("""Specify the size of the kernel
cache (in MB).""") cacheSize: Double = 100.0,
                           @ArgDoc("""Whether to use the shrinking heuristic.
Default is 1 (true).""") shrinking: Int = 1,
                           @ArgDoc("""Whether to enable probability estimates.
Default is 0 (false).""") probability: Int = 0,
                           @ArgDoc("""NR Weight""") nrWeight: Int = 1,
                           @ArgDoc("""Penalty parameter c of the error term.""") c: Double = 1.0,
                           @ArgDoc("""Set the epsilon in loss function of epsilon-SVR.""") p: Double = 0.1) {
  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(observationColumns != null && observationColumns.nonEmpty, "One or more observation columns is required")

  def getEpsilon: Double = {
    require(epsilon > 0.0, "epsilon must be a positive value")
    epsilon
  }

  def getDegree: Int = {
    require(degree > 0, "degree must be a positive value")
    degree
  }

  def getGamma: Double = {
    gamma.getOrElse(1 / observationColumns.length)
  }

  def getNu: Double = {
    nu
  }

  def getCoef0: Double = {
    coef
  }

  def getCacheSize: Double = {
    cacheSize
  }

  def getP: Double = {
    p
  }

  def getShrinking: Int = {
    shrinking
  }

  def getProbability: Int = {
    probability
  }

  def getNrWeight: Int = {
    nrWeight
  }

  def getC: Double = {
    c
  }

  def getSvmType: Int = {
    require(svmType >= 0 && svmType <= 4)
    svmType
  }

  def getKernelType: Int = {
    require(kernelType >= 0 && kernelType <= 3)
    kernelType
  }

  def getWeightLabel: Array[Int] = {
    weightLabel.getOrElse(Array[Int](0))
  }

  def getWeight: Array[Double] = {
    weight.getOrElse(Array[Double](0.0))
  }
}
