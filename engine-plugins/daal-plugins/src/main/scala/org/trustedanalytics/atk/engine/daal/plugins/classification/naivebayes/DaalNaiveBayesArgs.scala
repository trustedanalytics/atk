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

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

case class DaalNaiveBayesTrainArgs(model: ModelReference,
                                   @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                                   @ArgDoc("""Column containing the label for each
observation.""") labelColumn: String,
                                   @ArgDoc("""Column(s) containing the
observations.""") observationColumns: List[String],
                                   @ArgDoc("""Additive smoothing parameter
Default is 1.0.""") lambdaParameter: Double = 1.0,
                                   @ArgDoc("""Number of classes""") numClasses: Int = 2) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
  require(numClasses > 1, "number of classes must be greater than 1")
}

case class DaalNaiveBayesPredictArgs(model: ModelReference,
                                     @ArgDoc("""A frame whose labels are to be predicted.
By default, predict is run on the same columns over which the model is
trained.""") frame: FrameReference,
                                     @ArgDoc("""Column(s) containing the
observations whose labels are to be predicted.
By default, we predict the labels over columns the NaiveBayesModel
was trained on.""") observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")

}

case class DaalNaiveBayesTestArgs(model: ModelReference,
                                  @ArgDoc("""A frame whose labels are to be predicted.
By default, predict is run on the same columns over which the model is
trained.""") frame: FrameReference,
                                  @ArgDoc("""Column containing the actual
label for each observation.""") labelColumn: String,
                                  @ArgDoc("""Column(s) containing the
observations whose labels are to be predicted.
By default, we predict the labels over columns the NaiveBayesModel
was trained on.""") observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")

}

/**
 * JSON serialization for model
 */
object DaalNaiveBayesArgsFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._
  implicit val nbTrainArgsFormat = jsonFormat6(DaalNaiveBayesTrainArgs)
  implicit val nbPredictArgsFormat = jsonFormat3(DaalNaiveBayesPredictArgs)
  implicit val nbTestArgsFormat = jsonFormat4(DaalNaiveBayesTestArgs)
}