/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.regression

import org.apache.spark.ml.feature.Instance
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer

import scala.collection.Map
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg._

import scala.collection.mutable

import breeze.linalg.{ DenseVector => BDV, * }
import breeze.optimize.{ CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS }
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{ Experimental, Since }
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ Estimator, Model }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ DoubleType, StructType }
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ Accumulator, Accumulators, Logging, SparkException }

import scala.collection.mutable.ArrayBuffer

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

class Cox @Since("1.6.0") (@Since("1.6.0") override val uid: String)
    extends Estimator[CoxModel] with CoxParams
    with DefaultParamsWritable with Logging {

  def this() = this(Identifiable.randomUID("coxSurvReg"))

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

  /**
   * Extract [[featuresCol]], [[labelCol]] and [[censorCol]] from input dataset,
   * and put it in an RDD with strong types.
   */
  protected[ml] def extractCoxPoints(dataset: DataFrame, currentBeta: BDV[Double]): RDD[CoxPointWithCumulativeSumAndBetaX] = {
    val coxRdd = dataset.select($(featuresCol), $(labelCol), $(censorCol)).map {
      case Row(features: Vector, time: Double, censor: Double) =>
        CoxPoint(features, time, censor)
    }
    val sortedData = coxRdd.sortBy(_.time, false)
    val sc = sortedData.sparkContext
    val riskSetRdd = riskSet(sortedData, currentBeta)
    val rRdd = riskSetRdd.map(x => x._1)
    val cumulativeSum = computePartitionSum(rRdd)
    val broadCastCumulativeSum = sc.broadcast(cumulativeSum)
    val finalRisk = computeFinalR(riskSetRdd, broadCastCumulativeSum)
    val updatedCoxPoint = sortedData.zip(finalRisk).map { case (a, (nR, dR)) => CoxPointWithCumulativeSumAndBetaX(a.features, a.time, a.censor, nR, dR) }
    updatedCoxPoint
  }

  def computePartitionSum(rdd: RDD[Double]): scala.collection.Map[Int, Double] = {
    var map = rdd.mapPartitionsWithIndex {
      case (index, partition) => {
        val y = (index + 1, partition.sum)
        Iterator(y)
      }
    }.collectAsMap()
    map += (0 -> 0d)
    map
  }

  def riskSet(sortedData: RDD[CoxPoint], currentBeta: BDV[Double]): RDD[(Double, BDV[Double])] = {
    val X = sortedData.mapPartitionsWithIndex {
      case (i, iter) =>
        var sumR: Double = 0.0

        val featureBuf = new ArrayBuffer[(Double, BDV[Double])]()
        while (iter.hasNext) {
          val xj: BDV[Double] = new BDV(iter.next().features.toArray)
          val r = math.exp(currentBeta.dot(xj))

          sumR += r

          xj :*= r
          val sumTuple = (sumR, xj)
          featureBuf += sumTuple
        }
        featureBuf.iterator
    }
    X
  }

  def computeFinalR(riskSetRdd: RDD[(Double, BDV[Double])], broadcast: Broadcast[Map[Int, Double]]): RDD[(Double, BDV[Double])] = {
    riskSetRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        val prevSum = broadcast.value.getOrElse(i, throw new IllegalArgumentException("Previous sum not computed."))
        val featureBuf = new ArrayBuffer[(Double, BDV[Double])]()
        while (iter.hasNext) {
          val (sumR, xjR) = iter.next()
          val updatedSum = sumR + prevSum
          val sumTuple = (updatedSum, xjR)

          featureBuf += sumTuple
        }
        featureBuf.iterator
    }

  }

  override def fit(dataSet: DataFrame): CoxModel = {
    //validateAndTransformSchema(dataSet.schema, fitting = true)
    val numFeatures = dataSet.select($(featuresCol)).take(1)(0).getAs[Vector](0).size

    // Computing the mean of the observations
    val instanceRdd: RDD[Instance] = dataSet.select(col($(featuresCol))).map {
      case Row(features: Vector) =>
        Instance(0d, 1d, features)
    }

    val instanceRddArray = instanceRdd.collect()

    val meanSummarizer = {
      val seqOp = (c: MultivariateOnlineSummarizer,
        instance: Instance) =>
        {
          c.add(instance.features)
          c
        }

      val combOp = (c1: MultivariateOnlineSummarizer,
        c2: MultivariateOnlineSummarizer) =>
        {
          (c1.merge(c2))
          c1
        }

      instanceRdd.treeAggregate(new MultivariateOnlineSummarizer)(seqOp, combOp)
    }

    val meanVector = meanSummarizer.mean

    import breeze.linalg._
    import breeze.numerics._
    val initialBetas = DenseVector.zeros[Double](numFeatures)
    val instances = extractCoxPoints(dataSet, initialBetas)
    val handlePersistence = dataSet.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val costFun = new CoxCostFun(instances)
    val optimizer = new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))

    /*
       The parameters vector has three parts:
       the first element: Double, log(sigma), the log of scale parameter
       the second element: Double, intercept of the beta parameter
       the third to the end elements: Doubles, regression coefficients vector of the beta parameter
     */
    val initialParameters = DenseVector.zeros[Double](numFeatures)

    val states = optimizer.iterations(new CachedDiffFunction(costFun), initialParameters)

    val parameters = {
      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
        state = states.next()
        arrayBuilder += state.adjustedValue
      }
      if (state == null) {
        val msg = s"${optimizer.getClass.getName} failed."
        throw new SparkException(msg)
      }

      state.x.toArray.clone()
    }

    if (handlePersistence) instances.unpersist()

    val coefficients = Vectors.dense(parameters.slice(2, parameters.length))
    val model = new CoxModel(uid, coefficients, meanVector)
    copyValues(model.setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true)
  }

  override def copy(extra: ParamMap): Cox = defaultCopy(extra)
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

  def predict(features: Vector): Double = {
    val diffVector = features.toBreeze - meanVector.toBreeze
    val products = beta.toBreeze :* diffVector
    products.toArray.sum
  }

  //TODO:need to change this AB
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)
    val predictUDF = udf { features: Vector => predict(features) }
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
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: coefficients, intercept, scale
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

  // beta is the intercept and regression coefficients to the covariates
  private val beta = parameters

  private var totalCnt: Long = 0L
  private var lossSum = 0.0
  private var gradientBetaSum = BDV.zeros[Double](beta.length)
  private var gradientLogSigmaSum = 0.0

  def count: Long = totalCnt

  def loss: Double = if (totalCnt == 0) 1.0 else lossSum / totalCnt

  // Here we optimize loss function over beta and log(sigma)
  //TODO: Check with Soila
  def gradient: BDV[Double] = BDV.vertcat(BDV(Array(gradientLogSigmaSum / totalCnt.toDouble)),
    gradientBetaSum / totalCnt.toDouble)

  /**
   * Add a new training data to this CoxAggregator, and update the loss and gradient
   * of the objective function.
   * @param data The CoxPoint representation for one data point to be added into this aggregator.
   * @return This CoxAggregator object.
   */
  def add(data: CoxPointWithCumulativeSumAndBetaX): this.type = {
    val epsilon = math.log(data.cumulativeSum)

    val betaX: Double = beta.dot(data.features.toBreeze)
    lossSum += (betaX - epsilon) * data.censor

    data.betaDotX :*= 1 / data.cumulativeSum

    val temp = data.features.toBreeze - data.betaDotX
    gradientBetaSum += temp :* data.censor
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
    if (totalCnt != 0) {
      totalCnt += other.totalCnt
      lossSum += other.lossSum

      gradientBetaSum += other.gradientBetaSum
    }
    this
  }
}

/**
 * CoxCostFun implements Breeze's DiffFunction[T] for Cox cost.
 * It returns the loss and gradient at a particular point (parameters).
 * It's used in Breeze's convex optimization routines.
 */
private class CoxCostFun(sortedData: RDD[CoxPointWithCumulativeSumAndBetaX])
    extends DiffFunction[BDV[Double]] {

  override def calculate(currentBeta: BDV[Double]): (Double, BDV[Double]) = {

    val coxAggregator = sortedData.treeAggregate(new CoxAggregator(currentBeta))(
      seqOp = (c, v) => (c, v) match {
        case (aggregator, instance) => aggregator.add(instance)
      },
      combOp = (c1, c2) => (c1, c2) match {
        case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
      })

    (coxAggregator.loss, coxAggregator.gradient)
  }

}

/**
 * Class that represents the (features, time, censor) of a data point.
 *
 * @param features List of features for this data point.
 * @param time Label for this data point.
 * @param censor Indicator of the event has occurred or not. If the value is 1, it means
 *                 the event has occurred i.e. uncensored; otherwise censored.
 */
private[regression] case class CoxPoint(features: Vector, time: Double, censor: Double) {
  require(censor == 1.0 || censor == 0.0, "censor of class CoxPoint must be 1.0 or 0.0")
}

/**
 * Class that represents the (features, tume, censor, cummulativeSum and betaDotX)
 * @param features List of features for this data point.
 * @param time Label for this data point.
 * @param censor Indicator of the event has occurred or not. If the value is 1, it means
 *                 the event has occurred i.e. uncensored; otherwise censored.
 * @param cumulativeSum
 * @param betaDotX
 */
case class CoxPointWithCumulativeSumAndBetaX(features: Vector, time: Double, censor: Double, cumulativeSum: Double, betaDotX: BDV[Double])