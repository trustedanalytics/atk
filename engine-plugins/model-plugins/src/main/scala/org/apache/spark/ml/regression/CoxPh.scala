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

private[regression] trait CoxPhParams extends Params
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

class CoxPh(override val uid: String)
    extends Estimator[CoxPhModel] with CoxPhParams
    with DefaultParamsWritable with Logging {

  def this() = this(Identifiable.randomUID("coxPhSurvivalModel"))

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

  override def fit(dataFrame: DataFrame): CoxPhModel = {
    val numFeatures = dataFrame.select($(featuresCol)).take(1)(0).getAs[Vector](0).size

    val meanVector = computeFeatureMean(dataFrame)
    import breeze.linalg._
    val coxPhPointRdd = extractSortedCoxPhPointRdd(dataFrame)

    val handlePersistence = dataFrame.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) coxPhPointRdd.persist(StorageLevel.MEMORY_AND_DISK)
    val costFun = new CoxPhCostFun(coxPhPointRdd)

    var previousBeta = DenseVector.zeros[Double](numFeatures)
    var previousLoss = 1E-3
    var iterations: Int = 0
    var epsilon: Double = scala.Double.PositiveInfinity

    while (iterations < $(maxIter) && (epsilon > $(tol))) {
      val (currentLoss, currentGradient, currentInformationMatrix) = costFun.calculate(previousBeta)

      try {
        val realMatrix = breeze.linalg.pinv(currentInformationMatrix)
        val gradientTimesMatrixInverse: DenseMatrix[Double] = currentGradient.toDenseMatrix * realMatrix
        val updatedBetaAsMatrix: DenseMatrix[Double] = previousBeta.toDenseMatrix - gradientTimesMatrixInverse
        previousBeta = updatedBetaAsMatrix.toDenseVector
      }
      catch {
        case e: MatrixSingularException => throw new MatrixSingularException("Singular Matrix formed, cannot be inverted at iteration:" + iterations)
      }
      epsilon = math.abs(currentLoss - previousLoss)
      previousLoss = currentLoss
      iterations += 1
    }

    val coefficients = Vectors.dense(previousBeta.toArray)
    val model = new CoxPhModel(uid, coefficients, meanVector)
    copyValues(model.setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true)
  }

  override def copy(extra: ParamMap): CoxPh = defaultCopy(extra)

  /**
   * Extract [[featuresCol]], [[labelCol]] and [[censorCol]] from input dataFrame,
   * and put it in an RDD of CoxPoint sorted in descending order of time.
   */
  protected[ml] def extractSortedCoxPhPointRdd(dataFrame: DataFrame): RDD[CoxPhPoint] = {
    val rdd = dataFrame.select($(featuresCol), $(labelCol), $(censorCol)).map {
      case Row(features: Vector, time: Double, censor: Double) =>

        CoxPhPoint(features, time, censor)
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
        c1.merge(c2)
        c1
      }

      instanceRdd.treeAggregate(new MultivariateOnlineSummarizer)(seqOp, combOp)
    }

    meanSummarizer.mean
  }
}

object CoxPh extends DefaultParamsReadable[CoxPh] {

  override def load(path: String): CoxPh = super.load(path)
}

/**
 * Model produced by [[CoxPh]].
 */
class CoxPhModel(override val uid: String,
                 val beta: Vector,
                 val meanVector: Vector)
    extends Model[CoxPhModel] with CoxPhParams with MLWritable {

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

  override def copy(extra: ParamMap): CoxPhModel = {
    copyValues(new CoxPhModel(uid, beta, meanVector), extra)
      .setParent(parent)
  }

  override def write: MLWriter =
    new CoxPhModel.CoxPhModelWriter(this)
}

object CoxPhModel extends MLReadable[CoxPhModel] {

  override def read: MLReader[CoxPhModel] = new CoxPhModelReader

  override def load(path: String): CoxPhModel = super.load(path)

  /** [[MLWriter]] instance for [[CoxPhModel]] */
  private[CoxPhModel] class CoxPhModelWriter(instance: CoxPhModel) extends MLWriter with Logging {
    private case class Data(coefficients: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.beta)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class CoxPhModelReader extends MLReader[CoxPhModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[CoxPhModel].getName

    override def load(path: String): CoxPhModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("coefficients").head()
      val coefficients = data.getAs[Vector](0)
      val mean = data.getAs[Vector](1)
      val model = new CoxPhModel(metadata.uid, coefficients, mean)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

}

/**
 * CoxPhAggregator computes the loss, gradient and informationMatrix for a CoxPh loss function as used in CoxPh survival
 * analysis for samples in a dense vector in an online fashion.
 *
 * Two CoxPhAggregator can be merged together to have a summary of loss, gradient and information matrix of the
 * corresponding joint dataset.
 *
 * Given the values of the covariates x^{'}, for random lifetime t_{i} of subjects i = 1, ..., n, with corresponding
 * censoring censor_i, the log likelihood loss function under the CoxPh model is given as:
 * {
 *   L(\beta)=\sum_{i=1}^n[(\beta.x - log{\sum_{j=1}^Re^{\beta.x})})censor_i]
 * }
 * where R defines the risk-set R(t) is the set of all individuals i with t_i> t, i.e. the people who haven't died or been censored yet.
 *
 * The gradient vector is computed by taking the partial first order derivative of the above function with respect to beta_1, ..., beta_n
 *
 * The gradient vector of size 'k' is thus computed as:
 *   {
 *   G(\beta_k)=\sum_{i=1}^n[(x_i_k - (\frac{\sum_{j=1}^Re^{\beta_k.x_j_k}x_j_k}
 *                    {{\sum_{j=1}^Re^{\beta.x})}}))censor_i]
 *   }
 * The information matrix of dimensions k*k is given as :
 *
 *   I(a,b) = -\sum_{i=1}^n[\frac{(({\sum_{j=1}^Re^{\beta.x}})({\sum_{j=1}^Rx_j_ax_j_be^{\beta.x}}) -
 * ({\sum_{j=1}^Rx_j_ae^{\beta.x}})({\sum_{j=1}^Rx_j_be^{\beta.x}}))censor_i}{({\sum_{j=1}^Re^{\beta.x}})^2} ]
 *
 * @param parameters
 */
private class CoxPhAggregator(parameters: BDV[Double])
    extends Serializable {
  private val beta = parameters
  private var totalCnt: Long = 0L
  private var lossSum = 0.0
  private var gradientBetaSum = BDV.zeros[Double](beta.length)
  private var matrixSum = breeze.linalg.DenseMatrix.zeros[Double](beta.length, beta.length)

  def count: Long = totalCnt
  def loss: Double = lossSum
  def gradient: BDV[Double] = gradientBetaSum
  def informationMatrix: breeze.linalg.DenseMatrix[Double] = matrixSum

  /**
   * Add a new training data to this CoxPhAggregator, and update the loss and gradient
   * of the objective function.
   * @param data The CoxPhPoint representation for one data point to be added into this aggregator.
   * @return This CoxPhAggregator object.
   */
  def add(data: CoxPhPointWithMetaData): this.type = {
    val epsilon = math.log(data.sumEBetaX)
    val betaX: Double = beta.dot(data.features.toBreeze)

    if (data.censor != 0.0) {
      lossSum += (betaX - epsilon)
      gradientBetaSum += computeGradientVector(data)
      matrixSum += computeInformationMatrix(data)
    }
    totalCnt += 1
    this
  }

  /**
   * Compute the gradient for the given observation
   * @param data CoxPhPointWithMetaData storing the observation with it's risk set values
   * @return Breeze DenseVector storing the gradient values
   */
  def computeGradientVector(data: CoxPhPointWithMetaData): BDV[Double] = {
    val gradientVector = BDV.zeros[Double](beta.length)

    for (i <- 0 to beta.length - 1) {
      if (data.sumEBetaX != 0.0)
        gradientVector(i) = data.features(i) - data.sumXDotEBetaX(i) / data.sumEBetaX
      else
        gradientVector(i) = 0.0
    }
    gradientVector
  }

  /**
   * Compute the information matrix for the given observation
   * @param data CoxPhPointWithMetaData storing the observation with it's risk set values
   * @return BreezeDenseMatrix storing the Information Matrix values
   */
  def computeInformationMatrix(data: CoxPhPointWithMetaData): breeze.linalg.DenseMatrix[Double] = {
    val infoMatrix = breeze.linalg.DenseMatrix.zeros[Double](beta.length, beta.length)

    for (i <- 0 to beta.length - 1) {
      for (j <- 0 to beta.length - 1) {
        if (data.sumEBetaX != 0) {
          val numerator1 = -(data.sumEBetaX * data.sumXiXjEBetaX(i, j))
          val numerator2 = data.sumXDotEBetaX(i) * data.sumXDotEBetaX(j)
          val denominator = data.sumEBetaX * data.sumEBetaX
          infoMatrix(i, j) = (numerator1 + numerator2) / denominator
        }
        else
          infoMatrix(i, j) = 0.0
      }
    }
    infoMatrix
  }

  /**
   * Merge another CoxAggregator, and update the loss, gradient and information matrix
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other CoxPhAggregator to be merged.
   * @return This CoxPhAggregator object.
   */
  def merge(other: CoxPhAggregator): this.type = {
    totalCnt += other.totalCnt
    lossSum += other.lossSum
    gradientBetaSum += other.gradientBetaSum
    matrixSum += other.matrixSum
    this
  }
}

/**
 * CoxPhCostFun implements our distributed version of Newton Raphson for CoxPh cost.
 * It returns the loss, gradient and information matrix at a particular point (parameters).
 * It's used in Breeze's convex optimization routines.
 */
private class CoxPhCostFun(coxPhPointRdd: RDD[CoxPhPoint]) {

  def calculate(currentBeta: BDV[Double]): (Double, BDV[Double], breeze.linalg.DenseMatrix[Double]) = {

    val coxPhPointWithCumSumAndBetaX = extractCoxPhPointsWithMetaData(coxPhPointRdd, currentBeta)
    val coxPhAggregator = coxPhPointWithCumSumAndBetaX.treeAggregate(new CoxPhAggregator(currentBeta))(
      seqOp = (c, v) => (c, v) match {
        case (aggregator, instance) => aggregator.add(instance)
      },
      combOp = (c1, c2) => (c1, c2) match {
        case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
      })

    (coxPhAggregator.loss, coxPhAggregator.gradient, coxPhAggregator.informationMatrix)

  }

  /**
   * Computes additional parameters given CoxPhPoint and intial beta to be used by Newton Raphson to estimate new beta
   * @param coxPhPointRdd Rdd storing the CoxPhPoint containing features, time and censor
   * @param currentBeta The current value for beta
   * @return Rdd storing CoxPhPoint and sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXiXjEBetaX in addition
   */
  protected[ml] def extractCoxPhPointsWithMetaData(coxPhPointRdd: RDD[CoxPhPoint], currentBeta: BDV[Double]): RDD[CoxPhPointWithMetaData] = {

    val sc = coxPhPointRdd.sparkContext
    val riskSetRdd = riskSet(coxPhPointRdd, currentBeta)

    val rRdd = riskSetRdd.map(x => (x._1, x._4, x._5))

    val cumulativeSum = computePartitionSum(rRdd, currentBeta.length)
    val broadCastCumulativeSum = sc.broadcast(cumulativeSum)
    val finalRisk = computeFinalR(riskSetRdd, broadCastCumulativeSum)

    val updatedCoxPhPoint = coxPhPointRdd.zip(finalRisk).map { case (a, (sumR, xR, r, sumS, sumT)) => CoxPhPointWithMetaData(a.features, a.time, a.censor, sumR, xR, r, sumS, sumT) }

    updatedCoxPhPoint
  }

  /**
   * Returns the sum of each partition for the sumEBetaX, sumXEBetaX, sumXiXjEBetaX values
   * @param rdd Rdd containing for each observation the, sumEBetaX, sumXEBetaX, sumXiXjEBetaX
   * @param length The number of co-variates
   * @return Map storing, for each partition the sumEBetaX, sumXEBetaX, sumXiXjEBetaX values
   */
  def computePartitionSum(rdd: RDD[(Double, BDV[Double], breeze.linalg.DenseMatrix[Double])], length: Int): scala.collection.Map[Int, (Double, BDV[Double], breeze.linalg.DenseMatrix[Double])] = {
    import breeze.linalg.DenseVector
    val array = rdd.mapPartitionsWithIndex {
      case (index, iterator) => {
        var sumEBetaX = 0.0
        var sumXEBetaX = DenseVector.zeros[Double](length)
        var sumXiXjEBetaX = breeze.linalg.DenseMatrix.zeros[Double](length, length)
        while (iterator.hasNext) {
          val (partialSumEBetaX, partialSumXEBetaX, partialSumXiXjEBetaX) = iterator.next()
          sumEBetaX = partialSumEBetaX
          sumXEBetaX = partialSumXEBetaX
          sumXiXjEBetaX = partialSumXiXjEBetaX
        }
        val sumTuple = (index + 1, (sumEBetaX, sumXEBetaX, sumXiXjEBetaX))
        Array(sumTuple).toIterator
      }
    }.collect()

    val initTuple = (0, (0d, BDV.zeros[Double](length), breeze.linalg.DenseMatrix.zeros[Double](length, length)))
    val cumSum = array.scanLeft(initTuple)((x, y) => {
      val (xIndex, (xSumR, xSumS, xSumT)) = x
      val (yIndex, (ySumR, ySumS, ySumT)) = y

      (yIndex, (xSumR + ySumR, xSumS + ySumS, xSumT + ySumT))
    })
    cumSum.toMap
  }

  /**
   * Computes meta data using CoxPhPoint and current beta with one pass over the data
   * @param sortedData Rdd storing the features, time and censor information sorted in decreasing order on time
   * @param currentBeta The current beta value
   * @return Rdd containing the meta data as a tuple with sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXiXjEBetaX
   */
  def riskSet(sortedData: RDD[CoxPhPoint], currentBeta: BDV[Double]): RDD[(Double, BDV[Double], Double, BDV[Double], breeze.linalg.DenseMatrix[Double])] = {
    import breeze.linalg.DenseVector
    val metaData = sortedData.mapPartitionsWithIndex {
      case (i, iter) =>
        var sumEBetaX: Double = 0.0
        var sumXiEBetaX = DenseVector.zeros[Double](currentBeta.length)
        var sumXiXjEBetaX = breeze.linalg.DenseMatrix.zeros[Double](currentBeta.length, currentBeta.length)

        val featureBuf = new ArrayBuffer[(Double, BDV[Double], Double, BDV[Double], breeze.linalg.DenseMatrix[Double])]()
        while (iter.hasNext) {
          val x: BDV[Double] = new BDV(iter.next().features.toArray)
          val eBetaX = math.exp(currentBeta.dot(x))

          val xiXjEBetaX = breeze.linalg.DenseMatrix.zeros[Double](currentBeta.length, currentBeta.length)
          val xiEBetaX = x * eBetaX
          for (i <- 0 to currentBeta.length - 1) {
            for (j <- 0 to currentBeta.length - 1)
              xiXjEBetaX(i, j) = x(i) * x(j) * eBetaX
          }

          sumXiEBetaX = sumXiEBetaX + xiEBetaX
          sumEBetaX = sumEBetaX + eBetaX
          sumXiXjEBetaX = sumXiXjEBetaX + xiXjEBetaX

          val sumTuple = (sumEBetaX, xiEBetaX, eBetaX, sumXiEBetaX, sumXiXjEBetaX)
          featureBuf += sumTuple
        }
        featureBuf.iterator
    }
    metaData
  }

  /**
   * Computes the sum of sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXiXjEBetaX across all partitions
   * @param riskSetRdd Rdd containing the meta data as a tuple with sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXiXjEBetaX
   * @param broadcast Broadcast variable containing the sums of each partitions
   * @return Rdd of the sum of sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXiXjEBetaX across all partitions
   */
  def computeFinalR(riskSetRdd: RDD[(Double, BDV[Double], Double, BDV[Double], breeze.linalg.DenseMatrix[Double])],
                    broadcast: Broadcast[Map[Int, (Double, BDV[Double], breeze.linalg.DenseMatrix[Double])]]): RDD[(Double, BDV[Double], Double, BDV[Double], breeze.linalg.DenseMatrix[Double])] = {
    riskSetRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        val prevSumEBetaX = broadcast.value.getOrElse(i, throw new IllegalArgumentException("Previous sum e^beta.x not computed."))._1
        val prevSumXEBetaX = broadcast.value.getOrElse(i, throw new IllegalArgumentException("Previous sum x.e^beta.x not computed."))._2
        val prevSumXiXjEBetaX = broadcast.value.getOrElse(i, throw new IllegalArgumentException("Previous sum xi.xj.e^beta.x not computed"))._3
        val featureBuf = new ArrayBuffer[(Double, BDV[Double], Double, BDV[Double], breeze.linalg.DenseMatrix[Double])]()
        while (iter.hasNext) {
          val (sumEBetaX, xEBetaX, eBetaX, sumXEBetaX, sumXiXjEBetaX) = iter.next()
          val updatedSumEBetaX = sumEBetaX + prevSumEBetaX
          val updatedSumXEBetaX = sumXEBetaX + prevSumXEBetaX
          val updatedSumXiXjEBetaX = sumXiXjEBetaX + prevSumXiXjEBetaX
          val sumTuple = (updatedSumEBetaX, xEBetaX, eBetaX, updatedSumXEBetaX, updatedSumXiXjEBetaX)

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
private[regression] case class CoxPhPoint(features: Vector, time: Double, censor: Double) {
  require(censor == 1.0 || censor == 0.0, "censor of class CoxPhPoint must be 1.0 or 0.0")
}

/**
 *
 * @param features The covariates of the train data
 * @param time The time of the event
 * @param censor Value indicating if the event has occured. Can have 2 values: 0 - event did not happen (censored); 1 - event happened (not censored)
 * @param sumEBetaX Sum of e raised to the dot product of beta and features, for all observations in the risk set of an observation
 * @param xDotEBetaX Dot product of feature and e raised to the dot product of beta and features
 * @param eBetaX e raised to dot product of beta and features
 * @param sumXDotEBetaX Sum of Dot product of feature and e raised to the dot product of beta and features, for all observations in the risk set of an observation
 */
case class CoxPhPointWithMetaData(features: Vector,
                                  time: Double,
                                  censor: Double,
                                  sumEBetaX: Double,
                                  xDotEBetaX: BDV[Double],
                                  eBetaX: Double,
                                  sumXDotEBetaX: BDV[Double],
                                  sumXiXjEBetaX: breeze.linalg.DenseMatrix[Double])

