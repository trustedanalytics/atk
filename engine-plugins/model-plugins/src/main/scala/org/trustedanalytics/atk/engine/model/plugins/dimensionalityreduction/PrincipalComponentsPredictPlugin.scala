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
package org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation, ApiMaturityTag }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix, RowMatrix }
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vectors, Vector }
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._
import org.apache.spark.sql.Row

import scala.RuntimeException
import scala.collection.mutable.ListBuffer

@PluginDoc(oneLine = "Predict using principal components model.",
  extended = """Predicting on a dataframe's columns using a PrincipalComponents Model.""",
  returns =
    """A frame with existing columns and following additional columns:
      |'c' additional columns: containing the projections of V on the the frame
      |'t_squared_index': column storing the t-square-index value, if requested""".stripMargin)
class PrincipalComponentsPredictPlugin extends SparkCommandPlugin[PrincipalComponentsPredictArgs, FrameEntity] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:principal_components/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: PrincipalComponentsPredictArgs)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: PrincipalComponentsPredictArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //Running MLLib
    val principalComponentJsObject = model.dataOption.getOrElse(throw new RuntimeException("This model has not be trained yet. Please train before trying to predict"))
    val principalComponentData = principalComponentJsObject.convertTo[PrincipalComponentsData]

    validateInputArguments(arguments, principalComponentData)

    val c = arguments.c.getOrElse(principalComponentData.k)
    val predictColumns = arguments.observationColumns.getOrElse(principalComponentData.observationColumns)

    //create RDD from the frame
    val indexedFrameRdd = frame.rdd.zipWithIndex().map { case (row, index) => (index, row) }

    val indexedRowMatrix: IndexedRowMatrix = toIndexedRowMatrix(arguments.meanCentered, frame, principalComponentData, predictColumns, indexedFrameRdd)

    val eigenVectors = principalComponentData.vFactor
    val y = indexedRowMatrix.multiply(eigenVectors)
    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataType]()
    for (i <- 1 to c) {
      val colName = "p_" + i.toString
      columnNames += colName
      columnTypes += DataTypes.float64
    }
    val yNew = evaluateTSquaredIndex(arguments.tSquaredIndex, principalComponentData, y, columnNames, columnTypes)

    val resultFrameRdd = yNew.rows.map(row => (row.index, row.vector)).join(indexedFrameRdd)
      .map { case (index, (vector, row)) => Row.fromSeq(row.toSeq ++ vector.toArray.toSeq) }

    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = frame.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })
    val resultFrame = new FrameRdd(updatedSchema, resultFrameRdd)

    val resultFrameEntity = engine.frames.tryNewFrame(CreateEntityArgs(name = arguments.name, description = Some("created from principal components predict"))) {
      newFrame => newFrame.save(resultFrame)
    }

    resultFrameEntity

  }

  /**
   * Validate the arguments to the plugin
   * @param arguments Arguments passed to the predict plugin
   * @param principalComponentData Trained PrincipalComponents model data
   */
  def validateInputArguments(arguments: PrincipalComponentsPredictArgs, principalComponentData: PrincipalComponentsData): Unit = {
    if (arguments.meanCentered == true) {
      require(principalComponentData.meanCentered == arguments.meanCentered, "Cannot mean center the predict frame if the train frame was not mean centered.")
    }

    if (arguments.observationColumns.isDefined) {
      require(principalComponentData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    if (arguments.c.isDefined) {
      require(principalComponentData.k >= arguments.c.get, "Number of components must be at most the number of components trained on")
    }
  }

  /**
   * Compute an IndexedRowMatrix with/without t-squared index depending on the argument
   * @param tSquaredIndex Flag indicating whether we need to compute the t-squared index
   * @param principalComponentData Trained PrincipalComponents model data
   * @param y IndexedRowMatrix storing the projection into k dimensional space
   * @param columnNames ListBuffer storing the column name(s) of the output frame
   * @param columnTypes ListBuffer storing the column type(s) of the output frame
   * @return IndexedRowMatrix
   */
  def evaluateTSquaredIndex(tSquaredIndex: Boolean, principalComponentData: PrincipalComponentsData,
                            y: IndexedRowMatrix, columnNames: ListBuffer[String], columnTypes: ListBuffer[DataType]): IndexedRowMatrix = {
    tSquaredIndex match {
      case true => {
        val t = computeTSquaredIndex(y, principalComponentData.singularValues, principalComponentData.k)
        columnNames += "t_squared_index"
        columnTypes += DataTypes.float64
        t
      }
      case _ => y
    }
  }

  /**
   * Check flag and mean center the input RDD
   * @param meanCentered Flag indicating whether the frame is to be mean centered
   * @param frame
   * @param principalComponentData Trained PrincipalComponents model data
   * @param predictColumns Frame's column(s) to be used for principal components computation
   * @param indexedFrameRdd
   * @return
   */
  def toIndexedRowMatrix(meanCentered: Boolean, frame: SparkFrame, principalComponentData: PrincipalComponentsData,
                         predictColumns: List[String], indexedFrameRdd: RDD[(Long, Row)]): IndexedRowMatrix = {
    new IndexedRowMatrix(
      meanCentered match {
        case true => FrameRdd.toMeanCenteredIndexedRowRdd(indexedFrameRdd, frame.schema, predictColumns, principalComponentData.meanVector)
        case false => FrameRdd.toIndexedRowRdd(indexedFrameRdd, frame.schema, predictColumns)
      })
  }

  /**
   * Compute the t-squared index for an IndexedRowMatrix created from the input frame
   * @param y IndexedRowMatrix storing the projection into k dimensional space
   * @param E Singular Values
   * @param k Number of dimensions
   * @return IndexedRowMatrix with existing elements in the RDD and computed t-squared index
   */
  def computeTSquaredIndex(y: IndexedRowMatrix, E: Vector, k: Int): IndexedRowMatrix = {
    val matrix = y.rows.map(row => {
      val rowVectorToArray = row.vector.toArray
      var t = 0.0
      for (i <- 0 until k) {
        if (E(i) > 0)
          t += ((rowVectorToArray(i) * rowVectorToArray(i)) / (E(i) * E(i)))
      }
      new IndexedRow(row.index, Vectors.dense(rowVectorToArray :+ t))

    })
    new IndexedRowMatrix(matrix)

  }
}
