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
 * @param model Handle to the model to be written to.
 * @param frame Handle to the data frame
 * @param observationColumns Handle to the list of observation columns of the data frame
 * @param labelColumn Handle to the label column of the data frame
 */
case class ClassificationWithSGDTestArgs(model: ModelReference,
                                         @ArgDoc("""frame whose labels are to be
predicted.""") frame: FrameReference,
                                         @ArgDoc("""Column containing the actual
label for each observation.""") labelColumn: String,
                                         @ArgDoc("""Column(s) containing the observations
whose labels are to be predicted and tested.
Default is to test over the columns the SvmModel
was trained on.""") observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")

}
