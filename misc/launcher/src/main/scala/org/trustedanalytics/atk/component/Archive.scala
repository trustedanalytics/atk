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

package org.trustedanalytics.atk.component

import java.net.URL

import com.typesafe.config.{ ConfigParseOptions, ConfigFactory, Config }
import org.apache.commons.lang.StringUtils

import scala.reflect.ClassTag
import scala.reflect.io.{ File, Directory, Path }
import scala.util.Try

import ExceptionUtil.attempt

abstract class Archive(val definition: ArchiveDefinition, val classLoader: ClassLoader, config: Config)
    extends Component with ClassLoaderAware {

  //Component initialization
  init(definition.name, config)

  /**
   * Execute a code block using the `ClassLoader` defined in the {classLoader} member
   * rather than the class loader that loaded this class or the current thread class loader.
   */
  override def withMyClassLoader[T](expression: => T): T = withLoader(classLoader)(expression)

  /**
   * Load and initialize a `Component` based on configuration path.
   *
   * @param path the config path to look up. The path should contain a "class" entry
   *             that holds the string name of the class that should be instantiated,
   *             which should be a class that's visible from this archive's class loader.
   * @return an initialized and started instance of the component
   */
  protected def loadComponent(path: String): Component = {
    Archive.logger(s"Loading component $path (set ${SystemConfig.debugConfigKey} to true to enable component config logging")
    val className = configuration.getString(path.replace("/", ".") + ".class")
    val component = load(className).asInstanceOf[Component]
    val restricted = configuration.getConfig(path + ".config").withFallback(configuration).resolve()
    if (Archive.system.systemConfig.debugConfig) {
      Archive.logger(s"Component config for $path follows:")
      Archive.logger(restricted.root().render())
      Archive.logger(s"End component config for $path")
      FileUtil.writeFile(Archive.system.systemConfig.debugConfigFolder + path.replace("/", "_") + ".effective-conf",
        restricted.root().render())
    }
    component.init(path, restricted)
    component.start()
    component
  }

  /**
   * Called by archives in order to load new instances from the archive. Does not provide
   * any caching of instances.
   *
   * @param className the class name to instantiate and configure
   * @return the new instance
   */
  def load(className: String): Any = {
    Archive.logger(s"Archive ${definition.name} creating instance of class $className")
    withMyClassLoader {
      classLoader.loadClass(className).newInstance()
    }
  }

  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  def getAll[T: ClassTag](descriptor: String): Seq[T]

  /**
   * Obtain a single instance of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instance, or the first such instance if the locator provides more than one
   * @throws NoSuchElementException if no instances were found
   */
  def get[T: ClassTag](descriptor: String): T = getAll(descriptor).headOption
    .getOrElse(throw new NoSuchElementException(
      s"No class matching descriptor $descriptor was found in location '${definition.name}'"))

}

/**
 * Companion object for Archives.
 */
object Archive extends ClassLoaderAware {
  /**
   * Locations from which classes can be loaded.
   */
  sealed trait CodePath { def name: String; def path: Path }
  case class JarPath(name: String, path: Path) extends CodePath
  case class FolderPath(name: String, path: Path) extends CodePath

  /**
   * Can be set at runtime to use whatever logging framework is desired.
   */
  //TODO: load slf4j + logback in separate class loader (completely separate from this one) and use it.
  var logger: String => Unit = println
  private var systemState: SystemState = new SystemState()

  /**
   * Returns the requested archive, loading it if needed.
   * @param archiveName the name of the archive
   * @param className the name of the class managing the archive
   *
   * @return the requested archive
   */
  def getArchive(archiveName: String, className: Option[String] = None): Archive = {
    system.archive(archiveName).getOrElse(buildArchive(archiveName, className))
  }

  /**
   * Returns the class loader for the given archive
   *
   * @param archive the name of the archive
   * @return the class loader
   */
  def getClassLoader(archive: String): ClassLoader = {
    getArchive(archive).classLoader
  }

  /**
   * Return the jar file location for the specified archive
   * @param archive archive to search
   * @param f function for searching code paths that contain the archive
   */
  def getJar(archive: String, f: String => Array[URL] = a => getCodePathUrls(a)): URL = {
    val codePaths = f(archive)
    val jarPath = codePaths.find(u => u.getPath.endsWith(".jar"))
    jarPath match {
      case None => throw new Exception(s"Could not find jar file for $archive")
      case _ => jarPath.get
    }
  }

  /**
   * Search the paths to class folder or jar files for the specified archive
   * @param archive archive to search
   * @return Array of CodePaths to the found class folder and jar files
   */
  def getCodePaths(archive: String, sourceRoots: Array[String], jarFolders: Array[String]): Array[CodePath] = {
    //TODO: Sometimes ${PWD} doesn't get replaced by Config library, figure out why
    val paths = sourceRoots.map(s => s.replace("${PWD}", Directory.Current.get.toString()): Path).flatMap { root =>
      val baseSearchPath = Array(
        FolderPath("development class files", root / archive / "target" / "classes"),
        FolderPath("development resource files", root / archive / "src" / "main" / "resources"),
        JarPath("development jar", root / archive / "target" / (archive + ".jar")),
        FolderPath("engine development class files", root / "engine" / archive / "target" / "classes"),
        FolderPath("engine development resource files", root / "engine" / archive / "src" / "main" / "resources"),
        JarPath("engine development jar", root / "engine" / archive / "target" / (archive + ".jar")),
        FolderPath("plugins development class files", root / "engine-plugins" / archive / "target" / "classes"),
        FolderPath("plugins development resource files", root / "engine-plugins" / archive / "src" / "main" / "resources"),
        JarPath("plugins development jar", root / "engine-plugins" / archive / "target" / (archive + ".jar")),
        FolderPath("misc development class files", root / "misc" / archive / "target" / "classes"),
        FolderPath("misc development resource files", root / "misc" / archive / "src" / "main" / "resources"),
        JarPath("misc development jar", root / "misc" / archive / "target" / (archive + ".jar")),
        JarPath("yarn cluster mode", root / (archive + ".jar")), /* In yarn container mode, all jars are copied to root */
        JarPath("launcher", root / ".." / (archive + ".jar"))
      )
      archive match {
        case "engine-core" => baseSearchPath :+ JarPath("__spark__", root / "__spark__.jar")
        case _ => baseSearchPath
      }
    } ++ jarFolders.map(s => JarPath("deployed jar",
      (s.replace("${PWD}", Directory.Current.get.toString()): Path) / (archive + ".jar")))
    paths.foreach { p =>
      logger(s"Considering ${p.path}")
    }
    paths.filter {
      case JarPath(n, p) => File(p).exists
      case FolderPath(n, p) => Directory(p).exists
    }.toArray
  }

  /**
   * Search the paths to class folder or jar files for the specified archive
   * @param archive archive to search
   * @return Array of URLs to the found class folder and jar files
   */
  def getCodePathUrls(archive: String,
                      sourceRoots: Array[String] = system.systemConfig.sourceRoots,
                      jarFolders: Array[String] = system.systemConfig.jarFolders): Array[URL] = {
    getCodePaths(archive, sourceRoots, jarFolders).map { codePath =>
      logger(s"Found ${codePath.name} at ${codePath.path}")
      codePath.path.toURL
    }
  }

  /**
   * Create a class loader for the given archive, with the given parent.
   *
   * As a side effect, updates the loaders map.
   *
   * @param archive the archive whose class loader we're constructing
   * @param parentClassLoader the parent for the new class loader
   * @return a class loader
   */
  private[component] def buildClassLoader(archive: String,
                                          parentClassLoader: ClassLoader,
                                          sourceRoots: Array[String],
                                          jarFolders: Array[String],
                                          additionalPaths: Array[String] = Array.empty[String]): ClassLoader = {

    val urls = Archive.getCodePathUrls(archive, sourceRoots, jarFolders) ++ additionalPaths.map(normalizeUrl)
    require(urls.length > 0, s"Could not locate archive $archive")

    val loader = new ArchiveClassLoader(archive, urls, parentClassLoader)
    logger(s"Created class loader: $loader")
    loader
  }

  /**
   * Needed to make sure URLs are presented in a form that URLClassLoader can use (e.g. for files, they
   * need to start with file:// and, if they are directories, they need to end with a slash)
   */
  private def normalizeUrl(url: String): URL = {
    val lead = if (url.contains(":")) { StringUtils.EMPTY } else { "file://" }
    val tail = if ((lead + url).startsWith("file:") && !url.endsWith(File.separator) && !url.endsWith(".jar")) {
      File.separator
    }
    else { StringUtils.EMPTY }
    new URL(lead + url + tail)
  }

  def system: SystemState = systemState

  /**
   * Initializes an archive instance
   *
   * @param definition the definition (name, etc.)
   * @param classLoader  a class loader for the archive
   * @param augmentedConfig config that is specific to this archive
   * @param instance the (un-initialized) archive instance
   */
  private def initializeArchive(definition: ArchiveDefinition,
                                classLoader: ClassLoader,
                                augmentedConfig: Config,
                                instance: Archive) = {

    instance.init(definition.name, augmentedConfig)

    //Give each Archive a chance to clean up when the app shuts down
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        instance.stop()
      }
    })
  }

  def getAugmentedConfig(archiveName: String, loader: ClassLoader) = {
    val parseOptions = ConfigParseOptions.defaults()
    parseOptions.setAllowMissing(true)

    ConfigFactory.invalidateCaches()

    val augmentedConfig = system.systemConfig.rootConfiguration.withFallback(
      ConfigFactory.parseResources(loader, "reference.conf", parseOptions)
        .withFallback(system.systemConfig.rootConfiguration)).resolve()

    if (system.systemConfig.debugConfig)
      FileUtil.writeFile(system.systemConfig.debugConfigFolder + archiveName + ".effective-conf",
        augmentedConfig.root().render())

    augmentedConfig
  }

  /**
   * Main entry point for archive creation
   *
   * @param archiveName the archive to create
   * @return the created, running, `Archive`
   */
  private def buildArchive(archiveName: String,
                           className: Option[String] = None): Archive = {
    try {
      //First create a standard plugin class loader, which we will use to query the config
      //to see if this archive needs special treatment (i.e. a parent class loader other than the
      //defaultParentArchive class loader)

      val defaultClassLoader = buildClassLoader(archiveName,
        myClassLoader,
        system.systemConfig.sourceRoots,
        system.systemConfig.jarFolders
      )

      val augmentedConfigForDefaultClassLoader = ConfigFactory.defaultReference(defaultClassLoader)
      val systemConfigForDefaultClassLoader = new SystemConfig(augmentedConfigForDefaultClassLoader)

      // Default archive definition for current archive name
      val archiveDefinition = {
        val definition = ArchiveDefinition(archiveName, augmentedConfigForDefaultClassLoader)
        className match {
          case Some(name) => definition.copy(className = name)
          case _ => definition
        }
      }

      //Non-default parent class loader
      val parentArchive = archiveDefinition.parent
      val parentClassLoader = system.loader(parentArchive).getOrElse {
        if (archiveDefinition.name.equals(parentArchive) || StringUtils.EMPTY.equals(parentArchive)) {
          myClassLoader
        }
        else {
          getClassLoader(parentArchive)
        }
      }

      //Non-default archive class loader
      val archiveClassLoader = Archive.buildClassLoader(archiveName,
        parentClassLoader,
        system.systemConfig.sourceRoots,
        system.systemConfig.jarFolders,
        systemConfigForDefaultClassLoader.extraClassPath(archiveDefinition.configPath))

      createAndInitializeArchive(archiveName, archiveDefinition, archiveClassLoader)
    }
    catch {
      case e: Throwable => throw new ArchiveInitException("Exception while building archive: " + archiveName, e)
    }
  }

  private def createAndInitializeArchive(archiveName: String,
                                         archiveDefinition: ArchiveDefinition,
                                         archiveClassLoader: ClassLoader) = {

    val augmentedConfig = getAugmentedConfig(archiveName, archiveClassLoader)
    val archiveClass = attempt(archiveClassLoader.loadClass(archiveDefinition.className),
      s"Archive class ${archiveDefinition.className} not found")

    val constructor = attempt(archiveClass.getConstructor(classOf[ArchiveDefinition],
      classOf[ClassLoader],
      classOf[Config]),
      s"Class ${archiveDefinition.className} does not have a constructor of the form (ArchiveDefinition, ClassLoader, Config)")

    val instance = attempt(constructor.newInstance(archiveDefinition, archiveClassLoader, augmentedConfig),
      s"Loaded class ${archiveDefinition.className} in archive ${archiveDefinition.name}, but could not create an instance of it")

    val archiveInstance = attempt(instance.asInstanceOf[Archive],
      s"Loaded class ${archiveDefinition.className} in archive ${archiveDefinition.name}, but it is not an Archive")

    val restrictedConfig = Try { augmentedConfig.getConfig(archiveDefinition.configPath) }.getOrElse(ConfigFactory.empty())

    withLoader(archiveClassLoader) {

      initializeArchive(archiveDefinition, archiveClassLoader, restrictedConfig, archiveInstance)
      try {
        synchronized {
          val newSystemState = system.addArchive(archiveInstance)
          systemState = newSystemState
        }
        logger(s"Registered archive $archiveName with parent ${archiveDefinition.parent}")
        archiveInstance.start()
      }
      catch {
        case e: Exception => synchronized {
          throw e
        }
      }
    }

    archiveInstance
  }
}
