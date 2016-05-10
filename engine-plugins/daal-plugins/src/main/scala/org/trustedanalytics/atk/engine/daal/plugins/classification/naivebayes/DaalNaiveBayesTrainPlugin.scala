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

package org.trustedanalytics.atk.engine.daal.plugins.classification.naivebayes

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc, SparkCommandPlugin }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import DaalNaiveBayesModelFormat._
import DaalNaiveBayesArgsFormat._

@PluginDoc(oneLine = "Build a multinomial naive bayes model.",
  extended = """Train a DaalNaiveBayesModel using the observation column, label column of the train frame and an optional lambda value.""",
  returns = """dictionary
A dictionary with trained multinomial naive bayes model with the following keys:
'class_log_prior': smoothed empirical log probability for each class
'feature_log_prob': empirical log probability of features given a class, P(x_i|y)
            """)
class DaalNaiveBayesTrainPlugin extends SparkCommandPlugin[DaalNaiveBayesTrainArgs, DaalNaiveBayesTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_naive_bayes/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Run Intel DAAL NaiveBayes() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalNaiveBayesTrainArgs)(implicit invocation: Invocation): DaalNaiveBayesTrainReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    val naiveBayesModel = new DaalNaiveBayesTrainAlgorithm(
      frame.rdd,
      arguments.observationColumns,
      arguments.labelColumn,
      arguments.numClasses,
      arguments.lambdaParameter,
      arguments.classPrior
    ).train()

    model.data = naiveBayesModel.toJson.asJsObject
    DaalNaiveBayesTrainReturn(naiveBayesModel.classLogPrior, naiveBayesModel.featureLogProb)
  }
}
