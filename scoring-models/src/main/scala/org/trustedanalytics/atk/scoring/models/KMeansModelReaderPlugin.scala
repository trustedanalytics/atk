
package org.trustedanalytics.atk.scoring.models

import java.io._
import org.trustedanalytics.atk.scoring.models.ScoringJsonReaderWriters.KmeansModelFormat
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector, Vector }
import spray.json._

class KMeansModelReaderPlugin() extends ModelLoader {

  private var myKMeansModel: KMeansScoreModel = _

  override def load(bytes: Array[Byte]): Model = {
    try {
      val str = new String(bytes)
      println(str)
      val json: JsValue = str.parseJson
      val libKMeansModel = json.convertTo[KMeansModel]
      myKMeansModel = new KMeansScoreModel(libKMeansModel)
      myKMeansModel.asInstanceOf[Model]
    }
    catch {
      //TODO: log the error
      case e: IOException => throw e
    }
  }
}