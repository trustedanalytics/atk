/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.scoring

import java.io.{ FileOutputStream, File, FileInputStream }

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import spray.can.Http
import org.trustedanalytics.atk.model.publish.format.ModelPublishFormat
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.component.{ ArchiveDefinition, Archive }
import com.typesafe.config.{ Config, ConfigFactory }
import scala.reflect.ClassTag
import org.trustedanalytics.atk.scoring.interfaces.Model
import java.net.URI
import org.apache.commons.io.FileUtils

/**
 * Scoring Service Application - a REST application used by client layer to communicate with the Model.
 *
 * See the 'scoring_server.sh' to see how the launcher starts the application.
 */
class ScoringServiceApplication(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends Archive(archiveDefinition, classLoader, config) with EventLogging {

  EventLogging.raw = true
  info("Scoring server setting log adapter from configuration")

  EventLogging.raw = configuration.getBoolean("trustedanalytics.atk.scoring.logging.raw")
  info("Scoring server set log adapter from configuration")

  EventLogging.profiling = configuration.getBoolean("trustedanalytics.atk.scoring.logging.profile")
  info(s"Scoring server profiling: ${EventLogging.profiling}")

  //Direct subsequent archive messages to the normal log
  Archive.logger = s => info(s)
  Archive.logger("Archive logger installed")

  var archiveName: String = null
  var modelName: String = null
  var ModelBytesFileName: String = null

  /**
   * Main entry point to start the Scoring Service Application
   */
  override def start() = {
    val model = getModel
    val service = new ScoringService(model)
    createActorSystemAndBindToHttp(service)
  }

  private def getModel: Model = {

    var outputFile: FileOutputStream = null
    var tempTarFile: File = null

    try {
      var tarFilePath = config.getString("trustedanalytics.scoring-engine.archive-tar")
      if (tarFilePath.startsWith("hdfs://")) {
        val hdfsFileSystem: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(tarFilePath), new Configuration())
        tempTarFile = File.createTempFile("modelTar", ".tar")
        hdfsFileSystem.copyToLocalFile(false, new Path(tarFilePath), new Path(tempTarFile.getAbsolutePath))
        tarFilePath = tempTarFile.getAbsolutePath
      }
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>about to read the model")
      ModelPublishFormat.read(new File(tarFilePath), classLoader.getParent)
    }
    finally {
      FileUtils.deleteQuietly(tempTarFile)
    }
  }

  /**
   * We need an ActorSystem to host our application in and to bind it to an HTTP port
   */
  private def createActorSystemAndBindToHttp(scoringService: ScoringService): Unit = {
    // create the system
    implicit val system = ActorSystem("trustedanalytics-scoring")
    implicit val timeout = Timeout(5.seconds)
    val service = system.actorOf(Props(new ScoringServiceActor(scoringService)), "scoring-service")
    // Bind the Spray Actor to an HTTP Port
    // start a new HTTP server with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = config.getString("trustedanalytics.atk.scoring.host"), port = config.getInt("trustedanalytics.atk.scoring.port"))
    println("scoring server is running now")
  }

  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  override def getAll[T: ClassTag](descriptor: String): Seq[T] = {
    throw new Exception("API server provides no components at this time")
  }
}
