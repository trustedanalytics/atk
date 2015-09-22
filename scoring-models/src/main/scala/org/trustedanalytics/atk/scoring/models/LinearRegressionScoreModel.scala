
package org.trustedanalytics.atk.scoring.models

import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.trustedanalytics.atk.scoring.interfaces.Model

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class LinearRegressionScoreModel(linearRegressionModel: LinearRegressionModel) extends LinearRegressionModel(linearRegressionModel.weights, linearRegressionModel.intercept) with Model {

  override def score(data: Seq[Array[String]]): Seq[Any] = {
    var score = Seq[Any]()
    var value: Int = 2
    data.foreach { row =>
      {
        val x: Array[Double] = new Array[Double](row.length)
        row.zipWithIndex.foreach {
          case (value: Any, index: Int) => x(index) = atof(value)
        }
        score = score :+ (predict(Vectors.dense(x)) + 1)
      }
    }
    score
  }

  def atof(s: String): Double = {
    s.toDouble
  }

  def atoi(s: String): Int = {
    Integer.parseInt(s)
  }

}
