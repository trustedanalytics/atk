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
                                           @ArgDoc("""A frame to train the model on""") frame: FrameReference,
                                           @ArgDoc("""Column name containing the label for each observation""") labelColumn: String,
                                           @ArgDoc("""Column(s) containing the observations""") observationColumns: List[String],
                                           @ArgDoc("""Number of classes for classification. Default is 2.""") numClasses: Int = 2,
                                           @ArgDoc("""Number of tress in the random forest. Default is 1.""") numTrees: Int = 1,
                                           @ArgDoc("""Criterion used for information gain calculation. Supported values "gini" or "entropy". Default is "gini".""") impurity: String = "gini",
                                           @ArgDoc("""Maximum depth of the tree. Default is 4.""") maxDepth: Int = 4,
                                           @ArgDoc("""Maximum number of bins used for splitting features. Default is 100.""") maxBins: Int = 100,
                                           @ArgDoc("""Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed.""") seed: Int = scala.util.Random.nextInt(),
                                           @ArgDoc(
                                             """Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}.""") categoricalFeaturesInfo: Option[Map[Int, Int]] = None,
                                           @ArgDoc(
                                             """Number of features to consider for splits at each node. Supported values "auto","all","sqrt","log2","onethird.
If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all" ; if num_trees > 1, set to "sqrt"""") featureSubsetCategory: Option[String] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
  require(numTrees > 0, "numTrees must be greater than 0")
  require(maxDepth >= 0, "maxDepth must be non negative")
  require(numClasses >= 2, "numClasses must be at least 2")

  def getFeatureSubsetCategory: String = {
    var value = "all"
    value = featureSubsetCategory.getOrElse("all") match {
      case "auto" => {
        numTrees match {
          case 1 => "all"
          case _ => "sqrt"
        }
      }
      case x => x

    }
    value
  }

  def getCategoricalFeaturesInfo: Map[Int, Int] = {
    categoricalFeaturesInfo.getOrElse(Map[Int, Int]())
  }

}
