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
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

import MLLibJsonProtocol._
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

@PluginDoc(oneLine = "Predict using principal components model.",
  extended = """Predicting on a dataframe's columns using a PrincipalComponents Model.""",
  returns = """A dictionary containing a frame with existing columns and 'c' additional columns containing the projections of V on the the frame and the t-square-index value if requested""")
class PrincipalComponentsPredictPlugin extends SparkCommandPlugin[PrincipalComponentsPredictArgs, PrincipalComponentsPredictReturn] {

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
  override def execute(arguments: PrincipalComponentsPredictArgs)(implicit invocation: Invocation): PrincipalComponentsPredictReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //Running MLLib
    val principalComponentJsObject = model.dataOption.getOrElse(throw new RuntimeException("This model has not be trained yet. Please train before trying to predict"))
    val principalComponentData = principalComponentJsObject.convertTo[PrincipalComponentsData]

    if (arguments.observationColumns.isDefined) {
      require(principalComponentData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    if (arguments.c.isDefined) {
      require(principalComponentData.k >= arguments.c.get, "Number of components must be at most the number of components trained on")
    }

    val c = arguments.c.getOrElse(principalComponentData.k)
    val predictColumns = arguments.observationColumns.getOrElse(principalComponentData.observationColumns)

    //create RDD from the frame
//        val x  = arguments.meanCentered match {
//      case true => frame.rdd.toMeanCenteredVectorDenseRDD(predictColumns)
//      case false => frame.rdd.toVectorDenseRDD(predictColumns)
//    }
//    val i = x.zipWithIndex().map{case(row,index) => (index,row)}
//
//    val irm : IndexedRowMatrix = new IndexedRowMatrix()

    val indexedFrameRdd = frame.rdd.zipWithIndex().map { case (row, index) => (index, row) }

    val indexedRowMatrix : IndexedRowMatrix = new IndexedRowMatrix(
    arguments.meanCentered match {
      case true => { val frameToVectorRdd = frame.rdd.toVectorDenseRDD(predictColumns)
        val meanVector = Statistics.colStats(frameToVectorRdd).mean
        FrameRdd.toMeanCenteredIndexedRowRdd(indexedFrameRdd, frame.schema, predictColumns, meanVector)
      }
      case false => FrameRdd.toIndexedRowRdd(indexedFrameRdd, frame.schema,predictColumns)
    })


    //val indexedRowMatrix: IndexedRowMatrix = new IndexedRowMatrix(FrameRdd.toIndexedRowRdd(indexedFrameRdd, frame.schema, predictColumns))

    val eigenVectors = principalComponentData.vFactor
    val y = indexedRowMatrix.multiply(eigenVectors)

    val resultFrameRdd = y.rows.map(row => (row.index, row.vector)).join(indexedFrameRdd)
      .map { case (index, (vector, row)) => Row.fromSeq(row.toSeq ++ vector.toArray.toSeq) }

    val t = arguments.tSquareIndex.getOrElse(false) match {
      case true => Some(computeTSquareIndex(y.rows, principalComponentData.singularValues, principalComponentData.k))
      case _ => None
    }

    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataTypes.DataType]()
    for (i <- 1 to c) {
      val colName = "p_" + i.toString
      columnNames += colName
      columnTypes += DataTypes.float64
    }

    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = frame.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })
    val resultFrame = new FrameRdd(updatedSchema, resultFrameRdd)

    val resultFrameEntity = engine.frames.tryNewFrame(CreateEntityArgs(name = arguments.name, description = Some("created from principal components predict"))) {
      newFrame => newFrame.save(resultFrame)
    }
    PrincipalComponentsPredictReturn(resultFrameEntity, t)
  }

  def computeTSquareIndex(y: RDD[IndexedRow], E: Vector, k: Int): Double = {

    val squaredY = y.map(_.vector.toArray).map(elem => elem.map(x => x * x))
    val vectorOfAccumulators = for (i <- 0 to k - 1) yield y.sparkContext.accumulator(0.0)
    val acc = vectorOfAccumulators.toList
    val t = y.sparkContext.accumulator(0.0)
    squaredY.foreach(row => for (i <- 0 to k - 1) acc(i) += row(i))

    for (i <- 0 to k - 1) {
      if (E(i) > 0) {
        val div = acc(i).value / E(i)
        t += div
      }
    }
    t.value
  }

}
