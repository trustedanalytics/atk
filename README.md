ATK
===

#Setting up your build environment
You will need to increase your maven memory settings before running the maven build.

```
export MAVEN_OPTS="-Xmx512m -XX:PermSize=256m"
```

#Building
After cloning the repository (to a directory we'll refer to as 'atk'),
cd to atk/misc/titan-shaded and install the module.
```
cd atk/misc/titan-shaded/
mvn install

```

go back to the root of the repository
```
cd ../../
```

Start the zinc server for incremental compilation
```
bin/zinc.sh start
```

Compile source
```
mvn compile -P events
```


## To build all the jars necessary to run the rest server

```
mvn package -P events -DskipTests
```

If you want to run all the test run the maven package without skipTests option
```
mvn package -P events
```

You can add -T option to run maven with [parallel execution](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3) *except when running tests*.
```
#OK
mvn -T 4 package -P events -DskipTests
#OK
mvn -T 4 compile -P events


#Not OK, will fail
mvn -T 4 package -P events
```

# Running

This is an overview. Additional details and instructions are in the user documentation.

* Pre-requisites
  * Java 7
  * Access to Cloudera Hadoop cluster with Yarn, HDFS, HBase, Zookeeper
  * PostgreSQL
  * python 2.7
  * Python depedencies installed (numpy, bson, requests, etc)
* Build the project jars
* Setup conf/ folder
  * Create a conf/application.conf based on the templates under conf/examples
    * Add cluster settings in application.conf
  * Create a conf/logback.xml if you want to configure logging
* Use bin/rest-server.sh to start the server
* cd /python-client
* Open ipython or python shell
  * Verify you are runing a python 2.7
  * Run these commands first
    * import trustedanalytics as atk
    * atk.connect()

# Getting Help
* Gitter chat
* [Look at our end user docs](http://trustedanalytics.github.io/atk/)
* [Look at our wiki docs](../../wiki)

# Folders Overview
* bin/ - scripts for starting REST server and scoring engine, etc.
* conf/ - configuration templates for setting up a system, put your application.conf here for running out of source code
* deploy/ - a module for creating an uber jar needed for deployment on Analytics PaaS.
* doc/ - end user docs for the system (except doc for specific plugins goes in the plugins themselves)
* doc-api-examples/ - examples of api usage for plugins
* engine/
  * engine-core/ - loads and executes plugins and provides the basic services that plugins need.
  * interfaces/ - interfaces the Engine exposes to the REST server. (we should move plugin args/return values out of here)
  * graphbuilder - Titan graph construction and reading
  * meta-store/ - code that interacts with the meta-store database repository including SQL scripts
* engine-plugins/ - plugins use engine services to implement user visible operations
  * frame-plugins/ - frame related plugins, e.g. frame.add_columns()
  * graph-plugins/ - graph related plugins that run on Spark and GraphX
  * model-plugins/ - model related plugins, e.g. LinearRegressionModel
  * giraph-plugins/ - a few algorithms that run on Giraph
* integration-tests/ - developer written, build time integration tests in python, these run against a minimal version of our product
* misc/ - miscellaneous items that aren't really part of our product
  * launcher/ - starts up our application, launches parts of our app
* module-loader/ - future replacement for launcher, starts application and sets up ClassLoaders appropriately.
* package/ - packaging for VM's, RPM's
* python-client/ - python client code (talks with rest-server)
  * examples/ - example code for users of how to use the API
* rest-server/ - the rest server converts HTTP requests to messages that are sent to the Engine
* scoring-engine/ - a small lightweight REST server that can score against trained models
* scoring-interfaces/ - interfaces the scoring-engine depends on.  Other model implementations could be plugged in as 
  long as they implement these interfaces.
* scoring-models/ - implementations of models the scoring-engine uses.
* testutils/ - some test utility code that gets reused between tests in different modules


# Developer Todo
* Most items under misc should move to separate repos
* Enable lazy execution and delayed execution. We have a plan where SparkContexts can be re-used and only shutdown when needed.
* Properly support directed and undirected graphs (get rid of "bi-directional")
* Data types in graphs/frames needs to be extended greatly
* Improve Plugins
  * Further separation between framework and plugins (ideally these are even separate projects)
  * Possibly separate plugin from yarn job
  * Dependency Injection
  * Need many more extension points for 3rd parties
  * Meta-programming needs to be expanded to support more kinds of objects
  * Nicer interfaces for plugin authors
* All plugins should move to plugin modules (a few lingering plugins under engine-core)
* Add support for Spark's dataframes
* Replace Slick with something simpler (even straight JDBC would be better, everyone already knows SQL and we aren't getting enough value to justify the learning curve for Slick)
* Replace Spray DSL with something simpler (the DSL is confusing to use and our REST layer is too thin to make people learn a DSL)
* Integration tests need support added to be able to test Giraph and Titan functions
* testutils should probably merge into engine-core
* giraph-plugins needs refactoring of packages (the current hierarchy is very poorly organized)
* Need Maven profiles to make it easier for developers to build only part of the project
* Break up CommandExecutor
* Move args classes out of interfaces next to their associated plugin (possibly get rid of args classes all together)
* Auto-conversion of return types
* Frames should go back to mutable and immutability should be re-implemented but where frames can keep stable ids
* Launcher code needs simplification and tests (it doesn't seem to setup classloaders all of the way how we want)

