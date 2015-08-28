
package org.trustedanalytics.atk.scoring.models

import java.io._
import org.trustedanalytics.atk.scoring.models.ScoringJsonReaderWriters.LinearRegressionModelFormat
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }
import org.apache.spark.mllib.regression.LinearRegressionModel
import spray.json._
import spray.json.DefaultJsonProtocol._

class LinearRegressionModelReaderPlugin() extends ModelLoader {

  private var myLRMeansModel: LinearRegressionScoreModel = _

  override def load(bytes: Array[Byte]): Model = {
    try {
      val str = new String(bytes)
      println(str)
      val json: JsValue = str.parseJson
      val linearRegressionModel = json.convertTo[LinearRegressionModel]
      myLRMeansModel = new LinearRegressionScoreModel(linearRegressionModel)
      myLRMeansModel.asInstanceOf[Model]
    }
    catch {
      //TODO: log the error
      case e: IOException => throw e
    }
  }
}