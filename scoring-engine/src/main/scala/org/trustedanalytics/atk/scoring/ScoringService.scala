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

package org.trustedanalytics.atk.scoring

import akka.actor.Actor
import akka.event.Logging
import com.typesafe.config.ConfigFactory
import org.trustedanalytics.atk.scoring.interfaces.Model
import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol
import spray.http.MediaTypes._
import spray.http.{ StatusCodes, _ }
import spray.routing._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{ Failure, Success }

/**
 * We don't implement our route structure directly in the service actor because
 * we want to be able to test it independently, without having to spin up an actor
 *
 * @param scoringService the service to delegate to
 */
class ScoringServiceActor(val scoringService: ScoringService) extends Actor with HttpService {

  /**
   * the HttpService trait defines only one abstract member, which
   * connects the services environment to the enclosing actor or test
   */
  override def actorRefFactory = context

  /**
   * Delegates to Scoring Service.
   *
   * This actor only runs our route, but you could add other things here, like
   * request stream processing or timeout handling
   */
  def receive = runRoute(scoringService.serviceRoute)
}

/**
 * Defines our service behavior independently from the service actor
 */
class ScoringService(scoringModel: Model) extends Directives {

  lazy val description = {
    new ServiceDescription(name = "Trusted Analytics",
      identifier = "ia",
      versions = List("v1", "v2"))
  }

  val config = ConfigFactory.load(this.getClass.getClassLoader)
  import AtkDefaultJsonProtocol._
  implicit val descFormat = jsonFormat3(ServiceDescription)
  import ScoringServiceJsonProtocol._
  import spray.json._

  /**
   * Case class to hold model, model tar file path and json format protocol object
   *
   * @param model Scoring Model
   * @param modelPath Path of model on filesystem
   * @param jsonFormat DataOutputFormatJsonProtocol object
   */
  case class ModelData(model: Model, modelPath: String, jsonFormat: DataOutputFormatJsonProtocol)

  var modelData = ModelData(scoringModel,
    config.getString("trustedanalytics.scoring-engine.archive-tar"),
    new DataOutputFormatJsonProtocol(scoringModel))

  /**
   * Main Route entry point to the Scoring Server
   */
  val serviceRoute: Route = logRequest("scoring service", Logging.InfoLevel) {
    val prefix = "score"
    val metadataPrefix = "metadata"

    /**
     * Note:
     * 'modelData' varible should not be used directly in any of routes, instead it should be passed to
     *  a method and then use it. Rationale is - passing model object as an argument to method makes it 'val'
     *  by default which makes it immutable. Hence it does not get affected if revise model request is made
     *  simulteniously and modifies the 'modelData' object reference.
     */

    path("") {
      get {
        respondWithMediaType(`text/html`) {
          complete(
            getHomePage(modelData)
          )
        }
      }
    } ~
      path("v2" / prefix) {
        requestUri { uri =>
          post {
            entity(as[String]) {
              scoreArgs =>
                val json: JsValue = scoreArgs.parseJson
                getScore(modelData, json)
            }
          }
        }
      } ~
      path("v1" / prefix) {
        requestUri { uri =>
          parameterSeq { (params) =>
            val sr = params.toArray
            var records = Seq[Array[Any]]()
            for (i <- sr.indices) {
              val decoded = java.net.URLDecoder.decode(sr(i)._2, "UTF-8")
              val splitSegment = decoded.split(",")
              records = records :+ splitSegment.asInstanceOf[Array[Any]]
            }
            getScoreV1(modelData, records)
          }
        }
      } ~
      path("v2" / metadataPrefix) {
        requestUri { uri =>
          get {
            getMetaData(modelData)
          }
        }
      } ~
      path("v2" / "revise") {
        requestUri { uri =>
          post {
            entity(as[String]) {
              args =>
                this.synchronized {
                  val path = if (args.parseJson.asJsObject.getFields("model-path").size == 1) {
                    args.parseJson.asJsObject.getFields("model-path")(0).convertTo[String]
                  }
                  else {
                    null
                  }
                  //if data contains "force" parameter, then force switch shoudl be true, else false
                  val force = if (args.parseJson.asJsObject.getFields("force").size == 1) true else false
                  reviseModelData(modelData, path, force)
                }
            }
          }
        }
      }
  }

  private def getScore(md: ModelData, json: JsValue): Route = {
    onComplete(Future { scoreJsonRequest(md, json) }) {
      case Success(output) => complete(md.jsonFormat.DataOutputFormat.write(output).toString())
      case Failure(ex) => ctx => {
        ctx.complete(StatusCodes.InternalServerError, ex.getMessage)
      }
    }
  }

  private def getScoreV1(md: ModelData, records: Seq[Array[Any]]): Route = {
    onComplete(Future { scoreStringRequest(md, records) }) {
      case Success(string) => complete(string.mkString(","))
      case Failure(ex) => ctx => {
        ctx.complete(StatusCodes.InternalServerError, ex.getMessage)
      }
    }
  }

  private def getMetaData(md: ModelData): Route = {
    import spray.json._
    onComplete(Future { md.model.modelMetadata() }) {
      case Success(metadata) => complete(JsObject("model_details" -> metadata.toJson,
        "input" -> new JsArray(md.model.input.map(input => FieldFormat.write(input)).toList),
        "output" -> new JsArray(md.model.output.map(output => FieldFormat.write(output)).toList)).toString)
      case Failure(ex) => ctx => {
        ctx.complete(StatusCodes.InternalServerError, ex.getMessage)
      }
    }
  }

  private def getHomePage(md: ModelData): String = {
    val metadata = JsObject("model_details" -> md.model.modelMetadata().toJson,
      "input" -> new JsArray(md.model.input.map(input => FieldFormat.write(input)).toList),
      "output" -> new JsArray(md.model.output.map(output => FieldFormat.write(output)).toList)).prettyPrint

    s"""
      <html>
        <body>
          <h1>Welcome to the Scoring Engine</h1>
          <h3>Model details:</h3>
          Model Path: ${md.modelPath} <br>
          Model metadata:<pre> $metadata </pre>
        </body>
      </html>"""
  }

  private def reviseModelData(md: ModelData, modelPath: String, force: Boolean): Route = {
    if (modelPath == null) {
      complete(StatusCodes.BadRequest, "'model-path' is not present in request!")
    }
    else {
      try {
        val revisedModel = ScoringEngineHelper.getModel(modelPath)
        if (ScoringEngineHelper.isModelCompatible(modelData.model, revisedModel, force)) {
          modelData = ModelData(revisedModel, modelPath, new DataOutputFormatJsonProtocol(revisedModel))
          complete { """{"status": "success"}""" }
        }
        else {
          complete(StatusCodes.BadRequest, "Revised Model type or input-output parameters names are " +
            "different than existing model")
        }
      }
      catch {
        case e: Throwable =>
          modelData = md
          e.printStackTrace()
          if (e.getMessage.contains("File does not exist:")) {
            complete(StatusCodes.BadRequest, e.getMessage)
          }
          else {
            complete(StatusCodes.InternalServerError, e.getMessage)
          }
      }
    }
  }

  def scoreStringRequest(modelData: ModelData, records: Seq[Array[Any]]): Array[Any] = {
    records.map(row => {
      val score = modelData.model.score(row)
      score(score.length - 1).toString
    }).toArray
  }

  def scoreJsonRequest(modeldata: ModelData, json: JsValue): Array[Map[String, Any]] = {
    val records = modeldata.jsonFormat.DataInputFormat.read(json)
    records.map(row => scoreToMap(modeldata.model, modeldata.model.score(row))).toArray
  }

  def scoreToMap(model: Model, score: Array[Any]): Map[String, Any] = {
    val outputNames = model.output().map(o => o.name)
    require(score.length == outputNames.length, "Length of output values should match the output names")
    val outputMap: Map[String, Any] = outputNames.zip(score).map(combined => (combined._1.name, combined._2)).toMap
    outputMap
  }
}

case class ServiceDescription(name: String, identifier: String, versions: List[String])

