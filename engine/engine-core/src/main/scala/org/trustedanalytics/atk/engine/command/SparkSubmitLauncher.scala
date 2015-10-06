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

import org.trustedanalytics.atk.component.ClassLoaderAware
import org.trustedanalytics.atk.engine._
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.util.{ JvmMemory, KerberosAuthenticator }
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.domain.command.Command
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.event.EventLogging
import org.apache.commons.lang.StringUtils

/**
 * Our wrapper for calling SparkSubmit to run a plugin.
 *
 * First, SparkSubmitLauncher starts a SparkSubmit process.
 * Next, SparkSubmit starts a SparkCommandJob.
 * Finally, SparkCommandJob executes a SparkCommandPlugin.
 */
class SparkSubmitLauncher extends EventLogging with EventLoggingImplicits with ClassLoaderAware {

  def execute(command: Command, plugin: SparkCommandPlugin[_, _], archiveName: Option[String])(implicit invocation: Invocation): Int = {
    withContext("executeCommandOnYarn") {

      val tempConfFileName = s"/tmp/application_${command.id}.conf"
      val pluginArchiveName = archiveName.getOrElse(plugin.archiveName)

      // Serialize current config for the plugin so as to pass to Spark Submit
      val (pluginJarsList, pluginExtraClasspath) = plugin.serializePluginConfiguration(pluginArchiveName, tempConfFileName)

      try {

        withMyClassLoader {
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

          val pluginDependencyJars = EngineConfig.sparkAppJarsLocal match {
            case true => Array[String]() /* Expect jars to installed locally and available */
            case false => {
              val extraJarsForSparkSubmitValue = s"${EngineConfig.extraJarsForSparkSubmit}"
              val extraJarsForSparkSubmit = if (StringUtils.isEmpty(extraJarsForSparkSubmitValue))
                StringUtils.EMPTY
              else (extraJarsForSparkSubmitValue + ",")

              Array("--jars",
                s"${SparkContextFactory.jarPath("interfaces")}," +
                  s"${SparkContextFactory.jarPath("launcher")}," +
                  extraJarsForSparkSubmit +
                  s"${getPluginJarPath(pluginJarsList)}")
            }
          }

          val pythonDependencyPath = plugin.executesPythonUdf match {
            case true => "," + SparkContextFactory.getResourcePath("trustedanalytics.zip", Some(EngineConfig.pythonDefaultDependencySearchDirectories))
              .getOrElse(throw new RuntimeException("Default Python dependency trustedanalytics.zip was not found"))
            case false => ""
          }

          val pluginDependencyFiles = Array("--files", s"$tempConfFileName#application.conf$kerbFile$pythonDependencyPath",
            "--conf", s"config.resource=application.conf")
          val executionParams = Array(
            "--driver-java-options", s"-XX:MaxPermSize=${EngineConfig.sparkDriverMaxPermSize} $kerbOptions")

          val executorClassPathString = "spark.executor.extraClassPath"
          val dbLib: String = s"${EngineConfig.hiveLib}:${EngineConfig.jdbcLib}:"
          val dbConf: String = s"${EngineConfig.hiveConf}:"
          val executorSparkConf: String = s"${EngineConfig.sparkConfProperties.getOrElse(executorClassPathString, StringUtils.EMPTY)}"

          val executorClassPathTuple = EngineConfig.sparkAppJarsLocal match {
            case true => (executorClassPathString,
              s".:${SparkContextFactory.jarPath("interfaces")}:${SparkContextFactory.jarPath("launcher")}:" +
              dbLib + s"${getPluginJarPath(pluginJarsList, ":")}" +
              executorSparkConf)
            case false => (executorClassPathString,
              dbLib + executorSparkConf)
          }

          val driverClassPathString = "spark.driver.extraClassPath"
          val driverClassPathTuple = (driverClassPathString,
            s".:interfaces.jar:launcher.jar:engine-core.jar:frame-plugins.jar:graph-plugins.jar:model-plugins.jar:application.conf:" +
            s"${pluginExtraClasspath.mkString(":")}:" +
            dbLib +
            dbConf +
            s"${EngineConfig.sparkConfProperties.getOrElse(driverClassPathString, StringUtils.EMPTY)}")

          val executionConfigs = {
            for {
              (config, value) <- EngineConfig.sparkConfProperties + (executorClassPathTuple, driverClassPathTuple)
            } yield List("--conf", s"$config=$value")
          }.flatMap(identity).toArray

          val verbose = Array("--verbose")
          // Using engine-core.jar (or deploy.jar) here causes issue due to duplicate copying of the resource.
          // So we hack to submit the job as if we are spark-submit shell script
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

          val kerberosConfig = KerberosAuthenticator.getKerberosConfigJVMParam

          // Launch Spark Submit 
          info(s"Launching Spark Submit with InputArgs: ${inputArgs.mkString(" ")}")
          val pluginDependencyJarsStr = s"${SparkContextFactory.jarPath("engine-core")}:${pluginExtraClasspath.mkString(":")}"
          val javaArgs = kerberosConfig.isDefined match {
            case true => Array("java", kerberosConfig.get, "-cp", s"$pluginDependencyJarsStr", "org.apache.spark.deploy.SparkSubmit") ++ inputArgs
            case false => Array("java", "-cp", s"$pluginDependencyJarsStr", "org.apache.spark.deploy.SparkSubmit") ++ inputArgs
          }
          info(s"javaArgs: ${javaArgs.mkString(" ")}")

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
      }
      finally {
        Files.deleteIfExists(FileSystems.getDefault.getPath(s"$tempConfFileName"))
        sys.props -= "SPARK_SUBMIT" /* Removing so that next command executes in a clean environment to begin with */
      }
    }
  }

  private def getPluginJarPath(pluginJarsList: List[String], delimiter: String = ","): String = {
    pluginJarsList.map(j => SparkContextFactory.jarPath(j)).mkString(delimiter)
  }

}
