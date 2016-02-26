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

package org.trustedanalytics.atk.engine.daal.plugins.kmeans

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.daal.plugins.conversions.DaalConversionImplicits._

/** JSON conversion for arguments and return value case classes */
object DaalKMeansJsonFormat {
  implicit val daalKMeansArgsFormat = jsonFormat6(DaalKMeansTrainArgs)
  implicit val daalKmeansReturnArgsFormat = jsonFormat2(DaalKMeansTrainReturn)
  implicit val daalKmeansModelData = jsonFormat3(DaalKMeansModelData)
}

import DaalKMeansJsonFormat._

@PluginDoc(oneLine = "Creates DAAL KMeans Model from train frame.",
  extended = "Creating a DAAL KMeans Model using the observation columns.",
  returns = """dictionary
    A dictionary with trained KMeans model with the following keys\:
'centroids' : dictionary with 'Cluster:id' as the key and the corresponding centroid as the value
'assignments' : Frame with cluster assignments.""")
class DaalKMeansTrainPlugin extends SparkCommandPlugin[DaalKMeansTrainArgs, DaalKMeansTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_k_means/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Run DAAL k-means clustering algorithm on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   * as well as a function that can be called to produce a SparkContext that
   * can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalKMeansTrainArgs)(implicit invocation: Invocation): DaalKMeansTrainReturn = {
    val frame: SparkFrame = arguments.frame

    val modelData = DaalKMeansFunctions.trainKMeansModel(frame.rdd, arguments)

    //Writing the kmeansModel as JSON
    val model: Model = arguments.model
    //model.data = modelData.toJson.asJsObject

    //Get dictionary with centroids
    val centroids = modelData.centroids.zipWithIndex.map {
      case (centroid, i) =>
        ("Cluster:" + (i + 1).toString, centroid)
    }.toMap

    val assignmentFrame = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by DAAL kmeans train operation"))) {
      frameEntity: FrameEntity =>
        frameEntity.save(modelData.assignmentFrame)
    }
    DaalKMeansTrainReturn(centroids, assignmentFrame)
  }
}

