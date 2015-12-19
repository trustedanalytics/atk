ATK
===

#Setting up your build environment
You will need to increase your maven memory settings and set the https protocol.

```
export MAVEN_OPTS="-Xmx512m -XX:PermSize=256m -Dhttps.protocols=TLSv1.2"
```

You also need to add trustedanalytics.org certificate to your java keystore to download dependencies from our maven server.
```
wget https://s3-us-west-2.amazonaws.com/analytics-tool-kit/public.crt -O `pwd`/public.crt &&\
sudo keytool -importcert -trustcacerts -storepass changeit -file `pwd`/public.crt \
-alias trustedanalytics.org -noprompt \
-keystore `find -L $JAVA_HOME -name cacerts`
```
If you don't want to trust our public certificate you can change the all repository urls in parent [pom](pom.xml) from https://maven.trustedanalytics.org to http://maven.trustedanalytics.org 


#Building
After cloning the repository (to a directory we'll refer to as 'atk'),

Start the zinc server for incremental compilation
```
bin/zinc.sh start
```

Compile source
```
mvn compile
```


## To build all the jars necessary to run the rest server

```
mvn package -DskipTests
```

If you want to run all the test run the maven package without skipTests option
```
mvn package
```

You can add -T option to run maven with [parallel execution](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3) *except when running tests*.
```
#OK
mvn -T 4 package -DskipTests
#OK
mvn -T 4 compile


#Not OK, will fail
mvn -T 4 package
```

# Running

This is an overview. Additional details and instructions are in the user documentation.

* Pre-requisites
  * Maven 3.3.3
  * Java 8
  * Access to Cloudera Hadoop cluster with Yarn, HDFS, HBase, Zookeeper
  * PostgreSQL
  * python 2.7
  * [Python depedencies installed](package/config/trustedanalytics-python-client/requirements.txt)
* Build the project jars
* Setup conf/ folder
  * Create a conf/application.conf based on the templates under conf/examples
    * Add cluster settings in application.conf
  * Create a conf/logback.xml if you want to configure logging
* Use bin/rest-server.sh to start the server
* cd /python-client
* Open ipython or python shell
  * Verify you are running a python 2.7
  * Run these commands first
    * import trustedanalytics as ta
    * ta.connect()

# Getting Help
* [Bug Tracking](https://trustedanalytics.atlassian.net)
* [Look at our end user docs](http://trustedanalytics.github.io/atk/)
* [Look at our wiki docs](../../wiki)

# Folders Overview
* bin/ - scripts for starting REST server and scoring engine, etc.
* conf/ - configuration templates for setting up a system, put your application.conf here for running out of source code
* [doc](doc)/ - end user docs for the system (except doc for specific plugins goes in the plugins themselves)
* doc-api-examples/ - examples of api usage for plugins
* engine/
  * engine-core/ - loads and executes plugins and provides the basic services that plugins need.
  * interfaces/ - interfaces the Engine exposes to the REST server. (we should move plugin args/return values out of here)
  * meta-store/ - code that interacts with the meta-store database repository including SQL scripts
* engine-plugins/ - plugins use engine services to implement user visible operations
  * frame-plugins/ - frame related plugins, e.g. frame.add_columns()
  * graph-plugins/ - graph related plugins that run on Spark and GraphX
  * model-plugins/ - model related plugins, e.g. LinearRegressionModel
* integration-tests/ - developer written, build time integration tests in python, these run against a minimal version of our product
* misc/ - miscellaneous items that aren't really part of our product
* module-loader/ - starts application and sets up ClassLoaders appropriately.
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
* testutils should probably merge into engine-core
* Need Maven profiles to make it easier for developers to build only part of the project
* Break up CommandExecutor
* Move args classes out of interfaces next to their associated plugin (possibly get rid of args classes all together)
* Auto-conversion of return types
