
package org.trustedanalytics.atk.scoring.models

import java.util.StringTokenizer

import org.trustedanalytics.atk.scoring.interfaces.Model
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{ Vectors, DenseVector }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class LibKMeansModel(libKMeansModel: KMeansModel) extends KMeansModel(libKMeansModel.clusterCenters) with Model {

  override def score(data: Seq[Array[String]]): Future[Seq[Any]] = future {
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

  private def columnFormatter(valueIndexPairArray: Array[(Any, Int)]): String = {
    val result = for {
      i <- valueIndexPairArray
      value = i._1
      index = i._2
      if value != 0
    } yield s"$index:$value"
    s"${valueIndexPairArray(0)._1} ${result.mkString(" ")}"
  }

  def atof(s: String): Double = {
    s.toDouble
  }

  def atoi(s: String): Int = {
    Integer.parseInt(s)
  }

}
