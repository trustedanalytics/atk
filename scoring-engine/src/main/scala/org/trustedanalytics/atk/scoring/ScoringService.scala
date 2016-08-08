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
import org.trustedanalytics.atk.scoring.interfaces.Model
import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol
import spray.http.MediaTypes._
import spray.http._
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
  var model = scoringModel
  def homepage = {
    respondWithMediaType(`text/html`) {
      complete {
        <html>
          <body>
            <h1>Welcome to the Scoring Engine</h1>
          </body>
        </html>
      }
    }
  }

  lazy val description = {
    new ServiceDescription(name = "Trusted Analytics",
      identifier = "ia",
      versions = List("v1", "v2"))
  }

  import AtkDefaultJsonProtocol._
  implicit val descFormat = jsonFormat3(ServiceDescription)
  import ScoringServiceJsonProtocol._
  var jsonFormat = new DataOutputFormatJsonProtocol(model)
  import spray.json._

  /**
   * Main Route entry point to the Scoring Server
   */
  val serviceRoute: Route = logRequest("scoring service", Logging.InfoLevel) {
    val prefix = "score"
    val metadataPrefix = "metadata"
    path("") {
      get {
        homepage
      }
    } ~
      path("v2" / prefix) {
        requestUri { uri =>
          post {
            entity(as[String]) {
              scoreArgs =>
                val json: JsValue = scoreArgs.parseJson
                onComplete(Future { scoreJsonRequest(jsonFormat.DataInputFormat.read(json)) }) {
                  case Success(output) => complete(jsonFormat.DataOutputFormat.write(output).toString())
                  case Failure(ex) => ctx => {
                    ctx.complete(StatusCodes.InternalServerError, ex.getMessage)
                  }
                }
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
            onComplete(Future { scoreStringRequest(records) }) {
              case Success(string) => complete(string.mkString(","))
              case Failure(ex) => ctx => {
                ctx.complete(StatusCodes.InternalServerError, ex.getMessage)
              }
            }
          }
        }
      } ~
      path("v2" / metadataPrefix) {
        requestUri { uri =>
          get {
            import spray.json._
            onComplete(Future { model.modelMetadata() }) {
              case Success(metadata) => complete(JsObject("model_details" -> metadata.toJson,
                "input" -> new JsArray(model.input.map(input => FieldFormat.write(input)).toList),
                "output" -> new JsArray(model.output.map(output => FieldFormat.write(output)).toList)).toString())
              case Failure(ex) => ctx => {
                ctx.complete(StatusCodes.InternalServerError, ex.getMessage)

              }
            }
          }
        }
      } ~
      path("revise") {
        requestUri { uri =>
          post {
            entity(as[String]) {
              args =>
                try {
                  val path = args.parseJson.asJsObject.getFields("model-path")(0).convertTo[String]
                  model = ScoringEngineHelper.getModel(path)
                  jsonFormat = new DataOutputFormatJsonProtocol(model)
                  complete { """{"status": "success"}""" }
                }
                catch {
                  case e: Throwable =>
                    e.printStackTrace()
                    complete(StatusCodes.InternalServerError, e.getMessage)
                }
            }
          }
        }
      }
  }

  def scoreStringRequest(records: Seq[Array[Any]]): Array[Any] = {
    records.map(row => {
      val score = model.score(row)
      score(score.length - 1).toString
    }).toArray
  }

  def scoreJsonRequest(records: Seq[Array[Any]]): Array[Map[String, Any]] = {
    records.map(row => scoreToMap(model.score(row))).toArray
  }

  def scoreToMap(score: Array[Any]): Map[String, Any] = {
    val outputNames = model.output().map(o => o.name)
    require(score.length == outputNames.length, "Length of output values should match the output names")
    val outputMap: Map[String, Any] = outputNames.zip(score).map(combined => (combined._1.name, combined._2)).toMap
    outputMap
  }
}

case class ServiceDescription(name: String, identifier: String, versions: List[String])

