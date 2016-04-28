package org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{ Vectors, Vector }
import org.apache.spark.mllib.linalg.distributed.{ RowMatrix, IndexedRow, IndexedRowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Column, DataTypes }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.frame.SparkFrame

import scala.collection.mutable.ListBuffer

object PrincipalComponentsFunctions extends Serializable {
  /**
   * Validate the arguments to the plugin
   * @param arguments Arguments passed to the predict plugin
   * @param principalComponentData Trained PrincipalComponents model data
   */
  def validateInputArguments(arguments: PrincipalComponentsPredictArgs, principalComponentData: PrincipalComponentsData): Unit = {
    if (arguments.meanCentered) {
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
   * 
   * @param frameRdd
   * @param principalComponentData
   * @param predictColumns
   * @param c
   * @param meanCentered
   * @param computeTsquaredIndex
   * @return
   */
  def predictPrincipalComponents(frameRdd: FrameRdd,
                                 principalComponentData: PrincipalComponentsData,
                                 predictColumns: List[String],
                                 c: Int,
                                 meanCentered: Boolean,
                                 computeTsquaredIndex: Boolean): FrameRdd = {
    val indexedRowMatrix = toIndexedRowMatrix(frameRdd, predictColumns, meanCentered)
    val principalComponents = computePrincipalComponents(principalComponentData, c, indexedRowMatrix)

    val pcaColumns = for (i <- 1 to c) yield Column("p_" + i.toString, DataTypes.float64)
    val (componentColumns, components) = computeTsquaredIndex match {
      case true => {
        val tSquareMatrix = computeTSquaredIndex(principalComponents, principalComponentData.singularValues, c)
        val tSquareColumn = Column("t_squared_index", DataTypes.float64)
        (pcaColumns :+ tSquareColumn, tSquareMatrix)
      }
      case false => (pcaColumns, principalComponents)
    }

    val componentRows = components.rows.map(row => Row.fromSeq(row.vector.toArray.toSeq))
    val componentFrame = new FrameRdd(FrameSchema(componentColumns.toList), componentRows)
    frameRdd.zipFrameRdd(componentFrame)
  }


  /**
   * Compute principal components using trained model
   *
   * @param modelData Train model data
   * @param c Number of principal components to compute
   * @param indexedRowMatrix  Indexed row matrix with input data
   * @return Principal components with projection of input into k dimensional space
   */
  def computePrincipalComponents(modelData: PrincipalComponentsData,
                                 c: Int, 
                                 indexedRowMatrix: IndexedRowMatrix): IndexedRowMatrix = {
    val eigenVectors = modelData.vFactor
    val y = indexedRowMatrix.multiply(eigenVectors)
    val cComponentsOfY = new IndexedRowMatrix(y.rows.map(r => r.copy(vector = Vectors.dense(r.vector.toArray.take(c)))))
    cComponentsOfY
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
      var t = 0d
      for (i <- 0 until k) {
        if (E(i) > 0)
          t += ((rowVectorToArray(i) * rowVectorToArray(i)) / (E(i) * E(i)))
      }
      new IndexedRow(row.index, Vectors.dense(rowVectorToArray :+ t))
    })
    new IndexedRowMatrix(matrix)
  }

  /**
   * Convert frame to distributed row matrix
   *
   * @param frameRdd Input frame
   * @param columns List of columns names for creating row matrix
   * @param meanCentered If true, mean center the columns
   * @return Distributed row matrix
   */
  def toRowMatrix(frameRdd: FrameRdd, columns: List[String], meanCentered: Boolean): RowMatrix = {
    val vectorRdd = toVectorRdd(frameRdd, columns, meanCentered)
    new RowMatrix(vectorRdd)
  }

  /**
   * Convert frame to distributed indexed row matrix
   *
   * @param frameRdd Input frame
   * @param columns List of columns names for creating row matrix
   * @param meanCentered If true, mean center the columns
   * @return Distributed indexed row matrix
   */
  def toIndexedRowMatrix(frameRdd: FrameRdd, columns: List[String], meanCentered: Boolean): IndexedRowMatrix = {
    val vectorRdd = toVectorRdd(frameRdd, columns, meanCentered)
    new IndexedRowMatrix(vectorRdd.zipWithIndex().map { case (vector, index) => IndexedRow(index, vector) })
  }

  /**
   * Convert frame to vector RDD
   *
   * @param frameRdd Input frame
   * @param columns List of columns names for vector RDD
   * @param meanCentered If true, mean center the columns
   * @return Vector RDD
   */
  def toVectorRdd(frameRdd: FrameRdd, columns: List[String], meanCentered: Boolean): RDD[Vector] = {
    meanCentered match {
      case true => frameRdd.toMeanCenteredDenseVectorRDD(columns)
      case false => frameRdd.toDenseVectorRDD(columns)
    }
  }
}
