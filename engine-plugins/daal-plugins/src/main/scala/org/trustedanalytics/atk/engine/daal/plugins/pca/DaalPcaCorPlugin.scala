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

package org.trustedanalytics.atk.engine.daal.plugins.pca

import java.nio.DoubleBuffer

import com.intel.daal.algorithms.SparkPcaCor
import com.intel.daal.data_management.data.{ NumericTable, HomogenNumericTable }
import com.intel.daal.services.DaalContext
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation }
import org.trustedanalytics.atk.engine.daal.plugins.DaalDataConverters
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * Arguments for DAAL Principal Component Analysis plugin
 *
 * @param frame Input data frame
 * @param columnNames Column names
 */
case class DaalPcaCorArgs(frame: FrameReference,
                          columnNames: List[String]) {
  require(frame != null, "frame is required")
  require(columnNames != null && !columnNames.isEmpty, "column names should not be empty")
}

/** JSON conversion for arguments and return value case classes */
object DaalPcaCorJsonFormat {
  implicit val daalPcaFormat = jsonFormat2(DaalPcaCorArgs)
}
import DaalPcaCorJsonFormat._

/**
 * Plugin that executes Principal Component Analysis using Intel's Data Analytics Acceleration Library (DAAL)
 *
 * The plugin
 *
 */
class DaalPcaCorPlugin extends SparkCommandPlugin[DaalPcaCorArgs, FrameReference] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/daal_pca_cor"

  /** Disable Kryo serialization to prevent seg-faults when using DAAL */
  override def kryoRegistrator: Option[String] = None

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DaalPcaCorArgs)(implicit invocation: Invocation) = 2

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
  override def execute(arguments: DaalPcaCorArgs)(implicit invocation: Invocation): FrameReference = {

    // Load the ATK data frame
    val frame: SparkFrame = arguments.frame
    val frameRdd = frame.rdd
    val frameSchema = frame.rdd.frameSchema

    // Convert ATK data frame to DAAL numeric table
    val numericTableRdd = DaalDataConverters.convertFrameToNumericTableRdd(frameRdd, arguments.columnNames)

    // Compute Correlation PCA decomposition for dataRDD
    val context = new DaalContext()
    val pcaResults = SparkPcaCor.runPCA(context, new JavaPairRDD[Integer, HomogenNumericTable](numericTableRdd))

    // Convert PCA results to ATK data frame
    val pcaResultsRows = DaalDataConverters.convertPcaResultsToFrame(context, pcaResults.getEigenVectors(), pcaResults.getEigenValues())

    val pcaResultsFrameRdd = frameRdd.sparkContext.parallelize(pcaResultsRows)
    val newColumns = List(Column("eigen_vector", DataTypes.vector(arguments.columnNames.size)), Column("eigen_value", DataTypes.float64))
    val newSchema = FrameSchema(newColumns)

    context.dispose()

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by daal_pca_cor command"))) {
      newFrame: FrameEntity => newFrame.save(new FrameRdd(newSchema, pcaResultsFrameRdd))
    }

  }

}
