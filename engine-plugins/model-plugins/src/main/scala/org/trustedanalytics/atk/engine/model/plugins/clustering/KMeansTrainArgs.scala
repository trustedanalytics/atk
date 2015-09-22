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

package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for loading model data into existing model in the model database.
 */
case class KMeansTrainArgs(model: ModelReference,
                           @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                           @ArgDoc("""Columns containing the
observations.""") observationColumns: List[String],
                           @ArgDoc("""Column scalings for each of the observation columns.
The scaling value is multiplied by the corresponding value in the
observation column.""") columnScalings: List[Double],
                           @ArgDoc("""Desired number of clusters.
Default is 2.""") k: Option[Int] = None,
                           @ArgDoc("""Number of iterations for which the algorithm should run.
Default is 20.""") maxIterations: Option[Int] = None,
                           @ArgDoc("""Distance threshold within which we consider k-means to have converged.
Default is 1e-4.""") epsilon: Option[Double] = None,
                           @ArgDoc("""The initialization technique for the algorithm.
It could be either "random" or "k-means||".
Default is "k-means||".""") initializationMode: Option[String] = None) {
  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(columnScalings != null && columnScalings.nonEmpty, "columnWeights must not be null or empty")
  require(columnScalings.length == observationColumns.length, "Length of columnWeights and observationColumns needs to be the same")

  def getK: Int = {
    if (k.isDefined) {
      require(k.get > 0, "k must be at least 1")
    }
    k.getOrElse(2)
  }

  def getMaxIterations: Int = {
    if (maxIterations.isDefined) {
      require(maxIterations.get > 0, "maxIterations must be a positive value")
    }
    maxIterations.getOrElse(20)
  }

  def geteEpsilon: Double = {
    if (epsilon.isDefined) {
      require(epsilon.get > 0.0, "epsilon must be a positive value")
    }
    epsilon.getOrElse(1e-4)
  }

  def getInitializationMode: String = {
    initializationMode.getOrElse("k-means||")
  }


}

/**
 * Return object when training a KMeansModel
 * @param clusterSize A dictionary containing the number of elements in each cluster
 * @param withinSetSumOfSquaredError  Within cluster sum of squared distance
 */
case class KMeansTrainReturn(clusterSize: Map[String, Int], withinSetSumOfSquaredError: Double)
