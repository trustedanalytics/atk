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
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.trustedanalytics.atk.moduleloader.{ Module, Component }
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import org.trustedanalytics.atk.event.EventLogging
import com.typesafe.config.{ Config, ConfigFactory }
import scala.reflect.ClassTag
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }
import java.net.URI

/**
 * Scoring Service Application - a REST application used by client layer to communicate with the Model.
 *
 * See the 'scoring_server.sh' to see how the launcher starts the application.
 */
class ScoringServiceApplication extends Component with EventLogging {

  val config = ConfigFactory.load(this.getClass.getClassLoader)

  EventLogging.raw = true
  info("Scoring server setting log adapter from configuration")

  EventLogging.raw = config.getBoolean("trustedanalytics.atk.scoring.logging.raw")
  info("Scoring server set log adapter from configuration")

  EventLogging.profiling = config.getBoolean("trustedanalytics.atk.scoring.logging.profile")
  info(s"Scoring server profiling: ${EventLogging.profiling}")

  var moduleName: String = null
  var modelClassName: String = null
  var ModelBytesFileName: String = null

  /**
   * Main entry point to start the Scoring Service Application
   */
  override def start() = {

    modelLoadRead()

    lazy val modelLoader: ModelLoader = Module.load(moduleName, modelClassName)

    val service = initializeScoringServiceDependencies(modelLoader, ModelBytesFileName)

    createActorSystemAndBindToHttp(service)
  }

  /**
   * Stop this component
   */
  override def stop(): Unit = {
  }

  private def initializeScoringServiceDependencies(modelLoader: ModelLoader, modelFile: String): ScoringService = {
    val source = scala.io.Source.fromFile(modelFile)
    val byteArray = source.map(_.toByte).toArray
    source.close()
    val model = modelLoader.load(byteArray)
    new ScoringService(model)
  }

  private def modelLoadRead(): Unit = {

    var outputFile: FileOutputStream = null
    var myTarFile: TarArchiveInputStream = null
    try {
      var tarFilePath = config.getString("trustedanalytics.scoring-engine.archive-tar")
      if (tarFilePath.startsWith("hdfs://")) {
        val hdfsFileSystem: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(tarFilePath), new Configuration())
        val tempFilePath = "/tmp/models.tar"
        hdfsFileSystem.copyToLocalFile(false, new Path(tarFilePath), new Path(tempFilePath))
        tarFilePath = tempFilePath
      }
      val tmpPath = "/tmp/"
      myTarFile = new TarArchiveInputStream(new FileInputStream(new File(tarFilePath)))
      var entry: TarArchiveEntry = null
      entry = myTarFile.getNextTarEntry
      while (entry != null) {
        // Get the name of the file
        val individualFile: String = entry.getName
        // Get Size of the file and create a byte array for the size
        val content: Array[Byte] = new Array[Byte](entry.getSize.toInt)
        myTarFile.read(content, 0, content.length)
        outputFile = new FileOutputStream(new File(tmpPath + individualFile))
        IOUtils.write(content, outputFile)
        outputFile.close()

        // TODO: tar may have more than one jar

        if (individualFile.contains(".jar")) {
          moduleName = individualFile.substring(0, individualFile.indexOf(".jar"))
        }
        else if (individualFile.contains("modelname")) {
          val s = new String(content)
          modelClassName = s.replaceAll("\n", "")
          info("model name is " + modelClassName)
        }
        else {
          ModelBytesFileName = tmpPath + individualFile
        }
        entry = myTarFile.getNextTarEntry
      }
    }
    finally {
      IOUtils.closeQuietly(outputFile)
      IOUtils.closeQuietly(myTarFile)
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
  }

}
