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
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.plugin.ArgDoc

case class RandomForestClassifierTrainArgs(@ArgDoc("""Handle to the model to be used.""") model: ModelReference,
                                           @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                                           @ArgDoc("""Column name containing the label for each observation.""") labelColumn: String,
                                           @ArgDoc("""Column(s) containing the observations.""") observationColumns: List[String],
                                           @ArgDoc("""Number of classes for classification""") numClasses: Option[Int] = None,
                                           @ArgDoc("""Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}""") categoricalFeaturesInfo: Option[Map[Int, Int]] = None,
                                           @ArgDoc("""Number of tress in the random forest""") numTrees: Option[Int] = None,
                                           @ArgDoc("""Number of features to consider for splits at each node""") featureSubsetCategory: Option[String] = None,
                                           @ArgDoc("""Criterion used for information gain calculation""") impurity: Option[String] = None,
                                           @ArgDoc("""Maxium depth of the tree""") maxDepth: Option[Int] = None,
                                           @ArgDoc("""Maximum number of bins used for splitting features""") maxBins: Option[Int] = None,
                                           @ArgDoc("""Random seed for bootstrapping and choosing feature subsets""") seed: Option[Int] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")

  def getFeatureSubsetCategory: String = {
    var value = "all"
    value = featureSubsetCategory.getOrElse("all") match {
      case "auto" => {
        numTrees.getOrElse(1) match {
          case 1 => "all"
          case _ => "sqrt"
        }
      }
      case _ => featureSubsetCategory.getOrElse("all")

    }
    value
  }

  def getCategoricalFeaturesInfo: Map[Int, Int] = {
    categoricalFeaturesInfo.getOrElse(Map[Int, Int]())
  }

  def getNumTrees: Int = {
    numTrees.getOrElse(1)

  }

  def getNumClasses: Int = {
    numClasses.getOrElse(2)

  }

  def getImpurity: String = {
    impurity.getOrElse("gini")

  }

  def getMaxDepth: Int = {
    maxDepth.getOrElse(4)

  }

  def getMaxBins: Int = {
    maxBins.getOrElse(100)
  }

  def getSeed: Int = {
    seed.getOrElse(scala.util.Random.nextInt())
  }
}
