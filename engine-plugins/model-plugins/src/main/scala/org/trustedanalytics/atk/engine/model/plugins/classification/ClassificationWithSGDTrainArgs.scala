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

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for loading model data into existing model in the model database.
 */
case class ClassificationWithSGDTrainArgs(model: ModelReference,
                                          @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                                          @ArgDoc("""Column name containing the label
for each observation.""") labelColumn: String,
                                          @ArgDoc("""Column(s) containing the
observations.""") observationColumns: List[String],
                                          @ArgDoc("""The algorithm adds an intercept.
Default is true.""") intercept: Option[Boolean] = None,
                                          @ArgDoc("""Number of iterations.
Default is 100.""") numIterations: Option[Int] = None,
                                          @ArgDoc("""Step size for optimizer.
Default is 1.0.""") stepSize: Option[Int] = None,
                                          @ArgDoc("""Regularization L1 or L2.
Default is L2.""") regType: Option[String] = None,
                                          @ArgDoc("""Regularization parameter.
Default is 0.01.""") regParam: Option[Double] = None,
                                          @ArgDoc("""Mini batch fraction parameter.
Default is 1.0.""") miniBatchFraction: Option[Double] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")

  def getNumIterations: Int = {
    if (numIterations.isDefined) { require(numIterations.get > 0, "numIterations must be a positive value") }
    numIterations.getOrElse(100)
  }

  def getIntercept: Boolean = { intercept.getOrElse(true) }

  def getStepSize: Int = { stepSize.getOrElse(1) }

  def getRegParam: Double = { regParam.getOrElse(0.01) }

  def getMiniBatchFraction: Double = { miniBatchFraction.getOrElse(1.0) }

}
