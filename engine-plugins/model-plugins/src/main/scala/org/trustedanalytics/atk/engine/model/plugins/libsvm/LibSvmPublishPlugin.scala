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

import org.trustedanalytics.atk.engine.model.plugins.scoring.{ ModelPublish, ModelPublishArgs, ModelPublishJsonProtocol }
import ModelPublishJsonProtocol._
import org.apache.hadoop.fs.Path
import org.trustedanalytics.atk.domain.StringValue
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, _ }
import org.trustedanalytics.atk.engine.{ EngineConfig, HdfsFileStorage }
// Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import LibSvmJsonProtocol._
import libsvm.svm
import java.io.File
import org.apache.commons.io.FileUtils

/**
 * Rename columns of a frame
 */
@PluginDoc(oneLine = "Creates a scoring engine tar file.",
  extended = "The HDFS path to the tar file.")
class LibSvmPublishPlugin extends CommandPlugin[ModelPublishArgs, StringValue] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/publish"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ModelPublishArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ModelPublishArgs)(implicit invocation: Invocation): StringValue = {

    val model: Model = arguments.model

    //Extracting the LibSvmModel from the stored JsObject
    val libsvmData = model.data.convertTo[LibSvmData]
    val libsvmModel = libsvmData.svmModel
    var file: File = null

    try {
      file = File.createTempFile("tmp", ".txt")
      svm.svm_save_model(file.getAbsolutePath(), libsvmModel)
      val modelValues = FileUtils.readFileToString(file)

      StringValue(ModelPublish.createTarForScoringEngine(modelValues, "scoring-models", "org.trustedanalytics.atk.scoring.models.LibSvmModelReaderPlugin"))
    }
    catch { case ex: Exception => throw new RuntimeException(s"Unable to Publish the LibSvm Model\n" + ex.toString) }
    finally {
      FileUtils.deleteQuietly(file)
    }
  }

}
