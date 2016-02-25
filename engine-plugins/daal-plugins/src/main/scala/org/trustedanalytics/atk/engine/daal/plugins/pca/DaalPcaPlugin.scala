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


package org.trustedanalytics.atk.engine.daal.plugins.pca

import com.intel.daal.algorithms.pca.Method
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.{ ArgDocAnnotation, PluginDocAnnotation, EngineConfig }
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.daal.plugins.conversions.DaalConversionImplicits
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import DaalConversionImplicits._

/**
 * Arguments for DAAL Principal Component Analysis plugin
 *
 * @param frame Input data frame
 * @param columnNames Column names
 */
case class DaalPcaArgs(frame: FrameReference,
                       @ArgDoc("""The names of the column from which to compute PCA.""") columnNames: List[String],
                       @ArgDoc("""PCA method.
                           | cor - correlation.
                           | svd - singular value decomposition.
                         """) method: String = "cor") {
  require(frame != null, "frame is required")
  require(columnNames != null && columnNames.nonEmpty, "column names should not be empty")
  require(method == "cor" || method == "svd", "method must be 'svd' or 'cor'")

  def getPcaMethod(): Method = method.toLowerCase match {
    case "svd" => Method.svdDense
    case "cor" => Method.correlationDense
    case _ => throw new IllegalArgumentException(s"Unsupported PCA method: ${method}")
  }
}

case class DaalPcaReturn(observationColumns: List[String],
                         eigenValues: Array[Double],
                         eigenVectors: Array[Array[Double]])

/** JSON conversion for arguments and return value case classes */
object DaalPcaJsonFormat {
  implicit val daalPcaArgsFormat = jsonFormat3(DaalPcaArgs)
  implicit val daalPcaReturnArgsFormat = jsonFormat3(DaalPcaReturn)
}

import DaalPcaJsonFormat._

/**
 * Plugin that executes Principal Component Analysis using Intel's Data Analytics Acceleration Library (DAAL)
 */
@PluginDoc(oneLine = "Calculate principal components using Intel's Data Analytics Acceleration Library (DAAL).",
  extended = """Principal component analysis (PCA) [1]_ is used to transform a set of possibly correlated
variables into a smaller number of uncorrelated variables called principal components.
This method runs the DAAL implementation of Principal component analysis [2]_.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Principal_component_analysis
.. [2] https://software.intel.com/en-us/daal""",
  returns = "Frame with eigen values and eigen vectors.")
class DaalPcaPlugin extends SparkCommandPlugin[DaalPcaArgs, DaalPcaReturn] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/daal_pca"

  /** Disable Kryo serialization to prevent seg-faults when using DAAL */
  override def kryoRegistrator: Option[String] = None

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DaalPcaArgs)(implicit invocation: Invocation) = 2

  /** Tag indicating the maturity of the PCA plugin's API */
  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Executes Principal Component Analysis using Intel's Data Analytics Acceleration Library.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running  plugin
   * @return New frame containing the eigen vectors, and eigen valued computed by PCA
   */
  override def execute(arguments: DaalPcaArgs)(implicit invocation: Invocation): DaalPcaReturn = {
    DaalUtils.validateDaalLibraries(EngineConfig.daalDynamicLibraries)

    // Load the ATK data frame
    val frame: SparkFrame = arguments.frame
    val frameRdd = frame.rdd

    // Compute Correlation PCA decomposition for dataRDD
    val pcaResults = DaalPcaFunctions.runPCA(frameRdd, arguments)

    // Convert PCA results to ATK data frame
    val eigenVectors = pcaResults.loadings.toArrayOfDoubleArray()
    val eigenValues = pcaResults.scores.toDoubleArray()

    DaalPcaReturn(arguments.columnNames, eigenValues, eigenVectors)
  }

}
