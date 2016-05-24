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
package org.apache.spark.ml.regression

import breeze.linalg.{ DenseVector => BDV }
import breeze.optimize.{ CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS }
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ Estimator, Model }
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ DoubleType, StructType }
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.collection.{ Map, mutable }

private[regression] trait CoxParams extends Params
    with HasFeaturesCol with HasLabelCol with HasPredictionCol with HasMaxIter
    with HasTol with HasFitIntercept with Logging {

  /**
   * Param for censor column name.
   * The value of this column could be 0 or 1.
   * If the value is 1, it means the event has occurred i.e. uncensored; otherwise censored.
   * @group param
   */
  final val censorCol: Param[String] = new Param(this, "censorCol", "censor column name")

  /** @group getParam */
  def getCensorCol: String = $(censorCol)

  setDefault(censorCol -> "censor")

  /**
   * Validates and transforms the input schema with the provided param map.
   * @param schema input schema
   * @param fitting whether this is in fitting or prediction
   * @return output schema
   */
  protected def validateAndTransformSchema(
    schema: StructType,
    fitting: Boolean): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    if (fitting) {
      SchemaUtils.checkColumnType(schema, $(censorCol), DoubleType)
      SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
    }
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)

  }
}

class Cox(override val uid: String)
    extends Estimator[CoxModel] with CoxParams
    with DefaultParamsWritable with Logging {

  def this() = this(Identifiable.randomUID("coxSurvivalModel"))

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setCensorCol(value: String): this.type = set(censorCol, value)

  /** @group setParam */

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  setDefault(maxIter -> 100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  def setTol(value: Double): this.type = set(tol, value)

  setDefault(tol -> 1E-6)

  override def fit(dataFrame: DataFrame): CoxModel = {
    val numFeatures = dataFrame.select($(featuresCol)).take(1)(0).getAs[Vector](0).size

    val meanVector = computeFeatureMean(dataFrame)
    import breeze.linalg._
    val coxPointRdd = extractSortedCoxPointRdd(dataFrame)

    val handlePersistence = dataFrame.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) coxPointRdd.persist(StorageLevel.MEMORY_AND_DISK)
    val costFun = new CoxCostFun(coxPointRdd)

    var previousBeta = DenseVector.zeros[Double](numFeatures)
    var previousLoss = 1E-3
    var iterations: Int = 0
    var epsilon: Double = scala.Double.PositiveInfinity

    while (iterations < $(maxIter) && (epsilon > $(tol))) {
      val (currentLoss, currentGradient, currentInformationMatrix) = costFun.calculate(previousBeta)
      previousBeta = if (currentInformationMatrix == 0) previousBeta else previousBeta - (currentGradient / currentInformationMatrix)
      epsilon = math.abs(currentLoss - previousLoss)
      previousLoss = currentLoss
      iterations += 1
    }

    val coefficients = Vectors.dense(previousBeta.toArray)
    val model = new CoxModel(uid, coefficients, meanVector)
    copyValues(model.setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true)
  }

  override def copy(extra: ParamMap): Cox = defaultCopy(extra)

  /**
   * Extract [[featuresCol]], [[labelCol]] and [[censorCol]] from input dataFrame,
   * and put it in an RDD of CoxPoint sorted in descending order of time.
   */
  protected[ml] def extractSortedCoxPointRdd(dataFrame: DataFrame): RDD[CoxPoint] = {
    val rdd = dataFrame.select($(featuresCol), $(labelCol), $(censorCol)).map {
      case Row(features: Vector, time: Double, censor: Double) =>

        CoxPoint(features, time, censor)
    }
    rdd.sortBy(_.time, false)
  }
  /**
   * Computes a vector storing the mean of each of the columns given in [[featuresCol]]] of the dataFrame
   */
  protected[ml] def computeFeatureMean(dataFrame: DataFrame): org.apache.spark.mllib.linalg.Vector = {
    // Computing the mean of the observations
    val instanceRdd: RDD[Instance] = dataFrame.select(col($(featuresCol))).map {
      case Row(features: Vector) =>
        Instance(0d, 1d, features)
    }

    val meanSummarizer = {
      val seqOp = (c: MultivariateOnlineSummarizer,
        instance: Instance) => {
        c.add(instance.features)
        c
      }

      val combOp = (c1: MultivariateOnlineSummarizer,
        c2: MultivariateOnlineSummarizer) => {
        (c1.merge(c2))
        c1
      }

      instanceRdd.treeAggregate(new MultivariateOnlineSummarizer)(seqOp, combOp)
    }

    meanSummarizer.mean
  }
}

object Cox extends DefaultParamsReadable[Cox] {

  override def load(path: String): Cox = super.load(path)
}

/**
 * Model produced by [[Cox]].
 */
class CoxModel(override val uid: String,
               val beta: Vector,
               val meanVector: Vector)
    extends Model[CoxModel] with CoxParams with MLWritable {

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def predict(features: Vector, meanVector: Vector): Double = {
    val diffVector = features.toBreeze - meanVector.toBreeze
    val products = beta.toBreeze :* diffVector
    math.exp(products.toArray.sum)
  }

  //TODO: Need to check transform, copy, write, read, load when submitting to Spark
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)
    val predictUDF = udf { features: Vector => predict(features, meanVector) }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false)
  }

  override def copy(extra: ParamMap): CoxModel = {
    copyValues(new CoxModel(uid, beta, meanVector), extra)
      .setParent(parent)
  }

  override def write: MLWriter =
    new CoxModel.CoxModelWriter(this)
}

object CoxModel extends MLReadable[CoxModel] {

  override def read: MLReader[CoxModel] = new CoxModelReader

  override def load(path: String): CoxModel = super.load(path)

  /** [[MLWriter]] instance for [[CoxModel]] */
  private[CoxModel] class CoxModelWriter(instance: CoxModel) extends MLWriter with Logging {

    private case class Data(coefficients: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.beta)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class CoxModelReader extends MLReader[CoxModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[CoxModel].getName

    override def load(path: String): CoxModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("coefficients").head()
      val coefficients = data.getAs[Vector](0)
      val mean = data.getAs[Vector](1)
      val model = new CoxModel(metadata.uid, coefficients, mean)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

}

private class CoxAggregator(parameters: BDV[Double])
    extends Serializable {
  //TODO: Need to update these calculations for multivariate implementation

  private val beta = parameters
  private var totalCnt: Long = 0L
  private var lossSum = 0.0
  private var secondOrderDerivative = 0.0
  private var gradientBetaSum = BDV.zeros[Double](beta.length)

  def count: Long = totalCnt
  def loss: Double = lossSum
  def gradient: BDV[Double] = gradientBetaSum
  def informationMatrix: Double = secondOrderDerivative
  /**
   * Add a new training data to this CoxAggregator, and update the loss and gradient
   * of the objective function.
   * @param data The CoxPoint representation for one data point to be added into this aggregator.
   * @return This CoxAggregator object.
   */
  def add(data: CoxPointWithMetaData): this.type = {
    val epsilon = math.log(data.sumEBetaX)
    val betaX: Double = beta.dot(data.features.toBreeze)

    lossSum += (betaX - epsilon) * data.censor

    val rhs: BDV[Double] = if (data.sumEBetaX == 0) BDV(0d) else data.sumXDotEBetaX :/ data.sumEBetaX
    val diff = data.features.toBreeze - rhs
    gradientBetaSum += diff :* data.censor

    val numeratorA: Double = data.sumXDotEBetaX.dot(data.sumXDotEBetaX)
    val numeratorB: Double = data.sumEBetaX * data.sumXSquaredEBetaX
    val numerator = numeratorA - numeratorB
    val value = if (data.sumEBetaX == 0) numerator * data.censor else (numerator / (data.sumEBetaX * data.sumEBetaX)) * data.censor
    secondOrderDerivative += value
    totalCnt += 1
    this
  }

  /**
   * Merge another CoxAggregator, and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other CoxAggregator to be merged.
   * @return This Coxggregator object.
   */
  def merge(other: CoxAggregator): this.type = {
    totalCnt += other.totalCnt
    lossSum += other.lossSum

    gradientBetaSum += other.gradientBetaSum
    secondOrderDerivative += other.secondOrderDerivative
    this
  }
}

/**
 * CoxCostFun implements our distributed version of Newton Raphson for Cox cost.
 * It returns the loss, gradient and information matrix at a particular point (parameters).
 * It's used in Breeze's convex optimization routines.
 */
private class CoxCostFun(coxPointRdd: RDD[CoxPoint]) {

  def calculate(currentBeta: BDV[Double]): (Double, BDV[Double], Double) = {

    val coxPointWithCumSumAndBetaX = extractCoxPointsWithMetaData(coxPointRdd, currentBeta)

    val coxAggregator = coxPointWithCumSumAndBetaX.treeAggregate(new CoxAggregator(currentBeta))(
      seqOp = (c, v) => (c, v) match {
        case (aggregator, instance) => aggregator.add(instance)
      },
      combOp = (c1, c2) => (c1, c2) match {
        case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
      })

    (coxAggregator.loss, coxAggregator.gradient, coxAggregator.informationMatrix)

  }

  /**
   * Computes additional parameters given CoxPoint and intial beta to be used by Newton Raphson to estimate new beta
   * @param coxPointRdd Rdd storing the CoxPoint containing features, time and censor
   * @param currentBeta The current value for beta
   * @return Rdd storing CoxPoint and sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXSquaredEBetaX in addition
   */
  protected[ml] def extractCoxPointsWithMetaData(coxPointRdd: RDD[CoxPoint], currentBeta: BDV[Double]): RDD[CoxPointWithMetaData] = {

    val sc = coxPointRdd.sparkContext
    val riskSetRdd = riskSet(coxPointRdd, currentBeta)
    val rRdd = riskSetRdd.map(x => (x._1, x._4, x._5))

    val cumulativeSum = computePartitionSum(rRdd, currentBeta.length)
    val broadCastCumulativeSum = sc.broadcast(cumulativeSum)
    val finalRisk = computeFinalR(riskSetRdd, broadCastCumulativeSum)

    val updatedCoxPoint = coxPointRdd.zip(finalRisk).map { case (a, (sumR, xR, r, sumS, sumT)) => CoxPointWithMetaData(a.features, a.time, a.censor, sumR, xR, r, sumS, sumT) }

    updatedCoxPoint
  }

  import breeze.linalg.DenseVector

  /**
   * Returns the sum of each partition for the sumEBetaX, sumXEBetaX, sumXSquaredEBetaX values
   * @param rdd Rdd containing for each observation the, sumEBetaX, sumXEBetaX, sumXSquaredEBetaX
   * @param length The number of co-variates
   * @return Map storing, for each partition the sumEBetaX, sumXEBetaX, sumXSquaredEBetaX values
   */
  def computePartitionSum(rdd: RDD[(Double, BDV[Double], Double)], length: Int): scala.collection.Map[Int, (Double, BDV[Double], Double)] = {
    //TODO: Consider replacing mapPartitionsWithIndex with accumulator in riskSet
    val array = rdd.mapPartitionsWithIndex {
      case (index, iterator) => {
        var sumEBetaX = 0.0
        var sumXSquaredEBetaX = 0.0
        var sumXEBetaX = DenseVector.zeros[Double](length)

        while (iterator.hasNext) {
          val (partialSumEBetaX, partialSumXEBetaX, partialSumXSquaredEBetaX) = iterator.next()
          sumEBetaX = partialSumEBetaX
          sumXEBetaX = partialSumXEBetaX
          sumXSquaredEBetaX = partialSumXSquaredEBetaX

        }
        val sumTuple = (index + 1, (sumEBetaX, sumXEBetaX, sumXSquaredEBetaX))
        Array(sumTuple).toIterator
      }
    }.collect()

    val initTuple = (0, (0d, BDV.zeros[Double](length), 0d))
    val cumSum = array.scanLeft(initTuple)((x, y) => {
      val (xIndex, (xSumR, xSumS, xSumT)) = x
      val (yIndex, (ySumR, ySumS, ySumT)) = y

      (yIndex, (xSumR + ySumR, xSumS + ySumS, xSumT + ySumT))
    })
    cumSum.toMap
  }

  /**
   * Computes meta data using CoxPoint and current beta with one pass over the data
   * @param sortedData Rdd storing the features, time and censor information sorted in decreasing order on time
   * @param currentBeta The current beta value
   * @return Rdd containing the meta data as a tuple with sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXSquaredEBetaX
   */
  def riskSet(sortedData: RDD[CoxPoint], currentBeta: BDV[Double]): RDD[(Double, BDV[Double], Double, BDV[Double], Double)] = {
    import breeze.linalg.DenseVector
    val metaData = sortedData.mapPartitionsWithIndex {
      case (i, iter) =>
        var sumEBetaX: Double = 0.0
        var sumXSquaredEBetaX: Double = 0.0
        var sumXEBetaX = DenseVector.zeros[Double](currentBeta.length)
        val featureBuf = new ArrayBuffer[(Double, BDV[Double], Double, BDV[Double], Double)]()
        while (iter.hasNext) {
          val xj: BDV[Double] = new BDV(iter.next().features.toArray)
          val eBetaX = math.exp(currentBeta.dot(xj))
          val xSquared: Double = xj.dot(xj)
          sumXSquaredEBetaX += xSquared * eBetaX
          sumEBetaX += eBetaX
          val xEBetaX: BDV[Double] = xj * eBetaX
          sumXEBetaX = xEBetaX + sumXEBetaX
          val sumTuple = (sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXSquaredEBetaX)
          featureBuf += sumTuple
        }
        featureBuf.iterator
    }
    metaData
  }

  /**
   * Computes the sum of sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXSquaredEBetaX across all partitions
   * @param riskSetRdd Rdd containing the meta data as a tuple with sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXSquaredEBetaX
   * @param broadcast Broadcast variable containing the sums of each partitions
   * @return Rdd of the sum of sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXSquaredEBetaX across all partitions
   */
  def computeFinalR(riskSetRdd: RDD[(Double, BDV[Double], Double, BDV[Double], Double)], broadcast: Broadcast[Map[Int, (Double, BDV[Double], Double)]]): RDD[(Double, BDV[Double], Double, BDV[Double], Double)] = {
    riskSetRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        val prevSumEBetaX = broadcast.value.getOrElse(i, throw new IllegalArgumentException("Previous sum e^beta.x not computed."))._1
        val prevSumXEBetaX = broadcast.value.getOrElse(i, throw new IllegalArgumentException("Previous sum x.e^beta.x not computed."))._2
        val prevSumXSquaredEBetaX = broadcast.value.getOrElse(i, throw new IllegalArgumentException("Previous sum x^2.e^beta.x not computed."))._3
        val featureBuf = new ArrayBuffer[(Double, BDV[Double], Double, BDV[Double], Double)]()
        while (iter.hasNext) {
          val (sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXSquaredEBetaX) = iter.next()
          val updatedSumEBetaX = sumEBetaX + prevSumEBetaX
          val updatedSumXEBetaX = sumXEBetaX + prevSumXEBetaX
          val updatedSumXSquaredEBetaX = sumXSquaredEBetaX + prevSumXSquaredEBetaX
          val sumTuple = (updatedSumEBetaX, xEBetaX, eBetaX, updatedSumXEBetaX, updatedSumXSquaredEBetaX)

          featureBuf += sumTuple
        }
        featureBuf.iterator
    }

  }
}

/**
 * Class that represents the (features, time, censor) of a data point.
 * @param features List of features for this data point.
 * @param time Label for this data point.
 * @param censor Indicator of the event has occurred or not. If the value is 1, it means
 *               the event has occurred i.e. uncensored; otherwise censored.
 */
private[regression] case class CoxPoint(features: Vector, time: Double, censor: Double) {
  require(censor == 1.0 || censor == 0.0, "censor of class CoxPoint must be 1.0 or 0.0")
}

/**
 *
 * @param features The covariates of the train data
 * @param time The time of the event
 * @param censor Value indicating if the event has occured. Can have 2 values: 0 - event did not happen (censored); 1 - event happened (not censored)
 * @param sumEBetaX Sum of e raised to the dot product of beta and features, for all observations in the risk set of an observation
 * @param xDotEBetaX Dot product of feature and e raised to the dot product of beta and features
 * @param eBetaX e raised to dot product of beta and features
 * @param sumXDotEBetaX Sum of Dot product of feature and e raised to the dot product of beta and features, for all observations in the risk set of an observatiiiion
 * @param sumXSquaredEBetaX Sum of Dot product of square of the feature and e raised to the dot product of beta and features, for all observations in the risk set of an observatiiiion
 */
case class CoxPointWithMetaData(features: Vector,
                                time: Double,
                                censor: Double,
                                sumEBetaX: Double,
                                xDotEBetaX: BDV[Double],
                                eBetaX: Double,
                                sumXDotEBetaX: BDV[Double],
                                sumXSquaredEBetaX: Double)
