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

package org.trustedanalytics.atk.engine.command

import java.io.File
import java.nio.file.{ FileSystems, Files }

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.trustedanalytics.atk.engine._
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.util.{ JvmMemory, KerberosAuthenticator }
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.domain.command.Command
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.moduleloader.Module

/**
 * Our wrapper for calling SparkSubmit to run a plugin.
 *
 * First, SparkSubmitLauncher starts a SparkSubmit process.
 * Next, SparkSubmit starts a SparkCommandJob.
 * Finally, SparkCommandJob executes a SparkCommandPlugin.
 */
class SparkSubmitLauncher(hdfsFileStorage: HdfsFileStorage) extends EventLogging with EventLoggingImplicits {

  def execute(command: Command, plugin: SparkCommandPlugin[_, _], moduleName: String)(implicit invocation: Invocation): Int = {
    withContext("executeCommandOnYarn") {

      try {
        //Requires a TGT in the cache before executing SparkSubmit if CDH has Kerberos Support
        KerberosAuthenticator.loginWithKeyTabCLI()
        val (kerbFile, kerbOptions) = EngineConfig.kerberosKeyTabPath match {
          case Some(path) => (s",$path",
            s"-Dtrustedanalytics.atk.engine.hadoop.kerberos.keytab-file=${new File(path).getName}")
          case None => ("", "")
        }

        val sparkMaster = Array(s"--master", s"${EngineConfig.sparkMaster}")
        val jobName = Array(s"--name", s"${command.getJobName}")
        val pluginExecutionDriverClass = Array("--class", "org.trustedanalytics.atk.engine.command.SparkCommandJob")

        val hdfsJars = hdfsFileStorage.hdfsLibs(Module.allJarNames(moduleName))
        val pluginDependencyJars = Array("--jars", hdfsJars.filter(_.endsWith(".jar")).mkString(","))

        // the pound symbol '#' is used to rename a file during upload e.g. "/some/path/oldname#newname"
        val pluginDependencyFiles = Array("--files", s"${EngineConfig.effectiveApplicationConf}#application.conf$kerbFile",
          "--conf", s"config.resource=application.conf")
        val executionParams = Array(
          "--driver-java-options", s"-XX:MaxPermSize=${EngineConfig.sparkDriverMaxPermSize} $kerbOptions")

        val executorClassPathString = "spark.executor.extraClassPath"
        val executorClassPathTuple = (executorClassPathString,
          s"${EngineConfig.hiveLib}:" + EngineConfig.jdbcLib +
            s"${EngineConfig.hiveConf}" +
          s":${EngineConfig.sparkConfProperties.getOrElse(executorClassPathString, "")}")

        val executionConfigs = {
          for {
            (config, value) <- EngineConfig.sparkConfProperties + executorClassPathTuple
          } yield List("--conf", s"$config=$value")
        }.flatMap(identity).toArray

        val verbose = Array("--verbose")

        val sparkInternalDriverClass = Array("spark-internal")
        val pluginArguments = Array(s"${command.id}")

        // Prepare input arguments for Spark Submit; Do not change the order
        val inputArgs = sparkMaster ++
          jobName ++
          pluginExecutionDriverClass ++
          pluginDependencyJars ++
          pluginDependencyFiles ++
          executionParams ++
          executionConfigs ++
          verbose ++
          sparkInternalDriverClass ++
          pluginArguments

        val engineClasspath = Module.allLibs("engine").mkString(":")

        // Launch Spark Submit
        val javaArgs = Array("java", "-cp", s"$engineClasspath", "org.apache.spark.deploy.SparkSubmit") ++ inputArgs
        info(s"Launching Spark Submit: ${javaArgs.mkString(" ")}")

        // We were initially invoking SparkSubmit main method directly (i.e. inside our JVM). However, only one
        // ApplicationMaster can exist at a time inside a single JVM. All further calls to SparkSubmit fail to
        // create an instance of ApplicationMaster due to current spark design. We took the approach of invoking
        // SparkSubmit as a standalone process (using engine.jar) for every command to get the parallel
        // execution in yarn-cluster mode.

        val pb = new java.lang.ProcessBuilder(javaArgs: _*)
        val result = pb.inheritIO().start().waitFor()
        info(s"Command ${command.id} completed with exitCode:$result, ${JvmMemory.memory}")
        result
      }
      finally {
        sys.props -= "SPARK_SUBMIT" /* Removing so that next command executes in a clean environment to begin with */
      }
    }
  }

}
