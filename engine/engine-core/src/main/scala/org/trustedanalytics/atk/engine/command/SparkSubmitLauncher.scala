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

package org.trustedanalytics.atk.engine.command

import java.io.File
import org.trustedanalytics.atk.domain.jobcontext.JobContextTemplate
import org.trustedanalytics.atk.engine._
import org.trustedanalytics.atk.engine.frame.PythonRddStorage
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.util.{ JvmMemory, KerberosAuthenticator }
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.domain.command.Command
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.moduleloader.Module
import org.trustedanalytics.atk.moduleloader.ClassLoaderAware

/**
 * Our wrapper for calling SparkSubmit to run a plugin.
 *
 * First, SparkSubmitLauncher starts a SparkSubmit process.
 * Next, SparkSubmit starts a SparkCommandJob.
 * Finally, SparkCommandJob executes a SparkCommandPlugin.
 */
class SparkSubmitLauncher(engine: Engine) extends EventLogging with EventLoggingImplicits with ClassLoaderAware {

  lazy val hdfsFileStorage: FileStorage = engine.asInstanceOf[EngineImpl].fileStorage

  def execute(command: Command, plugin: SparkCommandPlugin[_, _], moduleName: String)(implicit invocation: Invocation): Int = {
    withContext("executeCommandOnYarn") {

      try {

        // make sure hdfs libs have been uploaded
        BackgroundInit.waitTillCompleted

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
        val pluginDependencyJars = Array("--jars", hdfsJars.mkString(","))

        val pythonDependencyPath = plugin.executesPythonUdf match {
          case true => "," + PythonRddStorage.pythonDepZip
          case false => ""
        }

        // the pound symbol '#' is used to rename a file during upload e.g. "/some/path/oldname#newname"
        val confFile = EngineConfig.effectiveApplicationConf
        val pluginDependencyFiles = Array("--files", s"$confFile$kerbFile$pythonDependencyPath,${EngineConfig.daalDynamicLibraries}")
        val executionParams = Array(
          "--driver-java-options", s"-XX:MaxPermSize=${EngineConfig.sparkDriverMaxPermSize} $kerbOptions -Dconfig.resource=${EngineConfig.effectiveApplicationConfFileName}")

        // TODO: not sure why we need to include Hive libraries this way

        val executorClassPathString = "spark.executor.extraClassPath"
        val executorClassPathTuple = (executorClassPathString,
          s":${EngineConfig.sparkBroadcastFactoryLib}" +
          s":${EngineConfig.hiveLib}:" + EngineConfig.jdbcLib +
          s":${EngineConfig.hiveConf}:" + EngineConfig.hbaseConf +
          s":${EngineConfig.sparkConfProperties.getOrElse(executorClassPathString, "")}")

        val driverClassPathString = "spark.driver.extraClassPath"
        val driverClassPathTuple = (driverClassPathString,
          s":${EngineConfig.sparkBroadcastFactoryLib}" +
          s":${EngineConfig.hiveLib}:" + EngineConfig.jdbcLib +
          s":${EngineConfig.hiveConf}:" + EngineConfig.hbaseConf +
          s":${EngineConfig.sparkConfProperties.getOrElse(driverClassPathString, "")}")

        val executionConfigs = {
          for {
            (config, value) <- EngineConfig.sparkConfProperties + (executorClassPathTuple, driverClassPathTuple)
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

        val engineClasspath = Module.allLibs("engine").map(url => url.getPath).mkString(":")

        val kerberosConfig = KerberosAuthenticator.getKerberosConfigJVMParam

        // Launch Spark Submit
        val javaArgs = if (kerberosConfig.isDefined) {
          Array("java", kerberosConfig.get, "-cp", s"$engineClasspath", "org.apache.spark.deploy.SparkSubmit") ++ inputArgs
        }
        else {
          Array("java", "-cp", s"$engineClasspath", "org.apache.spark.deploy.SparkSubmit") ++ inputArgs
        }
        info(s"Launching Spark Submit: ${javaArgs.mkString(" ")}")

        // We were initially invoking SparkSubmit main method directly (i.e. inside our JVM). However, only one
        // ApplicationMaster can exist at a time inside a single JVM. All further calls to SparkSubmit fail to
        // create an instance of ApplicationMaster due to current spark design. We took the approach of invoking
        // SparkSubmit as a standalone process (using engine.jar) for every command to get the parallel
        // execution in yarn-cluster mode.

        val pb = new java.lang.ProcessBuilder(javaArgs: _*)
        val job = pb.inheritIO().start()
        createJobContext(command)
        val result = job.waitFor()
        info(s"Command ${command.id} completed with exitCode:$result, ${JvmMemory.memory}")
        result
      }
      finally {
        sys.props -= "SPARK_SUBMIT" /* Removing so that next command executes in a clean environment to begin with */
      }
    }
  }

  /**
   * Create a job context for command. (Later -- Create a job context upon checking the client_id + repl_id)
   * @param command Current Command being executed
   * @param invocation Current Invocation
   * @return
   */
  def createJobContext(command: Command)(implicit invocation: Invocation): Unit = {
    if (EngineConfig.keepYarnJobAlive) {
      withMyClassLoader {
        // TODO Insert a record in jobcontext table if request is coming from the same user_id/client_id - It should tend to
        // TODO go to the same yarn job
        val engineImpl = engine.asInstanceOf[EngineImpl]
        val jobContext = engineImpl.jobContextStorage.lookupByName(command.getJobName)
        // TODO By this time the yarn job has not even been created (i.e. has not reach the NEW Phase) - need to figure this out
        // TODO hence introducing a sleep for 5 seconds. Another way might be to populate this field after it gets launched in yarn
        Thread.sleep(5000)
        val result = jobContext.getOrElse {
          val yarnAppId = YarnUtils.getYarnJobId(command.getJobName)
          // Hardcoded Client Id for now
          val clientId = invocation.user.clientId.getOrElse("clientId")
          val newJobContextTemplate = new JobContextTemplate(invocation.user.user.id, command.getJobName, yarnAppId, clientId)
          engineImpl.jobContextStorage.create(newJobContextTemplate)
        }
        engineImpl.commandStorage.updateJobContextId(command.id, result.id)
      }
    }
  }

}
