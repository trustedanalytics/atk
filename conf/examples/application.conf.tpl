# This (application.conf.tpl) is a configuration template for the Trusted Analytics Toolkit.
# Copy this to application.conf and edit to suit your system.
# Comments begin with a '#' character.
# Default values are 'commented' out with //.
# To configure for your system, look for configuration entries below with the word
# REQUIRED in all capital letters - these
# MUST be configured for the system to work.

# BEGIN REQUIRED SETTINGS

trustedanalytics.atk {
  #bind address - change to 0.0.0.0 to listen on all interfaces
  //api.host = "127.0.0.1"

  #bind port
  //api.port = 9099

  # The host name for the Postgresql database in which the metadata will be stored
  # Postgresql
  //metastore.connection-postgresql.host = "invalid-postgresql-host"
  //metastore.connection-postgresql.port = 5432
  //metastore.connection-postgresql.database = "atk-metastore"
  //metastore.connection-postgresql.username = "atkuser"
  //metastore.connection-postgresql.password = "myPassword"
  metastore.connection-postgresql.url = "jdbc:postgresql://"${trustedanalytics.atk.metastore.connection-postgresql.host}":"${trustedanalytics.atk.metastore.connection-postgresql.port}"/"${trustedanalytics.atk.metastore.connection-postgresql.database}

  # This allows for the use of postgres for a metastore. Service restarts will not affect the data stored in postgres
  metastore.connection = ${trustedanalytics.atk.metastore.connection-postgresql}

  # This allows the use of an in memory data store. Restarting the rest server will create a fresh database and any
  # data in the h2 DB will be lost
  //metastore.connection = ${trustedanalytics.atk.metastore.connection-h2}

  datastore
    {

     # OrientDB
    //connection-orientdb.host = "invalid-orientdb-host"
    //connection-orientdb.port = "invalid-orientdb-port"
    //connection-orientdb.username = "invalid-orientdb-user"
    //connection-orientdb.password = "invalid-orientdb-password"
    //connection-orientdb.url = "remote:"${trustedanalytics.atk.datastore.connection-orientdb.host}":"${trustedanalytics.atk.datastore.connection-orientdb.port}
  }

  engine {

    # The hdfs URL where the trustedanalytics folder will be created
    # and which will be used as the starting point for any relative URLs
    fs.root = "hdfs://invalid-fsroot-host/user/atkuser"

    # The location for checkpointing, usually under the filesystem root
    fs.checkpoint-directory = ${trustedanalytics.atk.engine.fs.root}"/checkpoints"

    # The URL for connecting to the Spark master server
    spark.master = "spark://invalid-spark-master:7077"

    # dynamic libraries for Intel Data Analytics Acceleration Library (Intel DAAL)
    spark.daal.dynamic-libraries=${DAAL_LIB_DIR}"/libAtkDaalJavaAPI.so,"${DAAL_LIB_DIR}"/libiomp5.so,"${DAAL_LIB_DIR}"/libJavaAPI.so,"${DAAL_LIB_DIR}"/"${DAAL_GCC_VERSION}"/libtbb.so.2"

    spark.conf.properties {
      # Memory should be same or lower than what is listed as available in Cloudera Manager.
      # Values should generally be in gigabytes, e.g. "64g"
      spark.executor.memory = "invalid executor memory"

      # These class paths need to be uncommented and be present on YARN nodes
      // spark.driver.extraClassPath = ":/opt/cloudera/parcels/CDH/jars/mysql-connector-java-5.1.23.jar:.:"
      // spark.executor.extraClassPath = ":/opt/cloudera/parcels/CDH/jars/mysql-connector-java-5.1.23.jar:postgresql-9.1-901.jdbc4.jar:"

      # Preferably spark.yarn.jar is installed in HDFS
      # In Cloudera Manager,
      #   1) Make sure the SPARK setting "spark_jar_hdfs_path" is set to this value
      #   2) Use "Actions" -> "Upload Spark Jar" to install jar in HDFS, if it is not already there
      //spark.yarn.jar = "hdfs://invalid-hdfs-host/user/spark/share/lib/spark-assembly.jar"
      //spark.yarn.jar = "/opt/cloudera/parcels/CDH/lib/spark/assembly/lib/spark-assembly-1.5.0-cdh5.5.0-hadoop2.6.0-cdh5.5.0.jar"
    }

    #Kerberos authentication configuration. if enabled is set to true will authenticate to kerberos
    //hadoop.kerberos {
    //  enabled = false
    //  principal-name = "my-user@MY.REALM.COM"
    //  keytab-file = "/path/to/keytab" #readable by atkuser
    //}
  }

}

# END REQUIRED SETTINGS

# The settings below are all optional. Some may need to be configured depending on the
# specifics of your cluster and workload.

trustedanalytics.atk {
  engine {
    auto-partitioner {
      # auto-partitioning spark based on the file size
      file-size-to-partition-size = [
        {upper-bound = "1MB", partitions = 15},
        {upper-bound = "1GB", partitions = 45},
        {upper-bound = "5GB", partitions = 100},
        {upper-bound = "10GB", partitions = 200},
        {upper-bound = "15GB", partitions = 375},
        {upper-bound = "25GB", partitions = 500},
        {upper-bound = "50GB", partitions = 750},
        {upper-bound = "100GB", partitions = 1000},
        {upper-bound = "200GB", partitions = 1500},
        {upper-bound = "300GB", partitions = 2000},
        {upper-bound = "400GB", partitions = 2500},
        {upper-bound = "600GB", partitions = 3750}
      ]

      # max-partitions is used if value is above the max upper-bound
      max-partitions = 10000

      repartition {
        # disabled - disable re-partitioning
        # frame_create_only - re-partition only during frame creation
        # shrink_only - re-partition during frame creation, and when the number partitions is less than existing partitions. Uses less-expensive Spark merge
        # shrink_or_grow - during frame creation, and either increase or decrease the number of partitions using more-expensive Spark shuffle
        #                  Using this option will also change the ordering of the frame during the shuffle
        strategy = "disabled"

        # percentage change in number of partitions that triggers re-partition
        threshold-percent = 80

        # Used to estimate actual size of the frame for compressed file formats like Parquet.
        # This ratio prevents us from under-estimating the number of partitions for compressed files.
        # compression-ratio=uncompressed-size/compressed-size
        # e.g., compression-ratio=4 if  uncompressed size is 20MB, and compressed size is 5MB
        frame-compression-ratio = 3
      }

      # used by some Spark plugins to set the number of partitions to default-tasks-per-core * number of Spark cores
      default-tasks-per-core = 2

      # use broadcast join if file size is lower than threshold. zero disables broadcast joins.
      # this threshold should be less than the maximum size of results returned to Spark driver (i.e., spark.driver.maxResultSize).
      # to increase Spark driver memory, edit java options (IA_JVM_OPT) in /etc/default/trustedanalytics-rest-server
      broadcast-join-threshold = "512MB"
    }
  }

  # Configuration for the Trusted Analytics REST server
  api {
    #this is reported by the REST server in the /info results - it can be used to identify
    #a particular server or cluster
    //identifier = "ia"

    #The default page size for result pagination
    //default-count = 20

    #Timeout for waiting for results from the engine
    //default-timeout = 30s

    #HTTP request timeout for the REST server
    //request-timeout = 29s
  }

  #Configuration for the IAT processing engine
  engine {
    //default-timeout = 30
    //page-size = 1000

    spark {

      # When master is empty the system defaults to spark://`hostname`:7070 where hostname is calculated from the current system
      # For local mode (useful only for development testing) set master = "local[4]"
      # in cluster mode, set master and home like the example
      # master = "spark://MASTER_HOSTNAME:7077"
      # home = "/opt/cloudera/parcels/CDH/lib/spark"

      # When home is empty the system will check expected locations on the local system and use the first one it finds
      # If spark is running in yarn-cluster mode (spark.master = "yarn-cluster"), spark.home needs to be set to the spark directory on CDH cluster
      # ("/usr/lib/spark","/opt/cloudera/parcels/CDH/lib/spark/", etc)
      //home = ""

      # Path to jar with LargeObjectTorrentBroadcastFactory which fixes bug in Spark when broadcast variables exceed 2GB.
      # TODO: Remove jar once fix is incorporated in Spark
      //broadcast-factory-lib = "spark-broadcast-1.3.jar"

      conf {
        properties {
          # These key/value pairs will be parsed dynamically and provided to SparkConf()
          # See Spark docs for possible values http://spark.apache.org/docs/0.9.0/configuration.html
          # All values should be convertible to Strings

          #Examples of other useful properties to edit for performance tuning:

          # Increased Akka frame size from default of 10MB to 100MB to allow tasks to send large results to Spark driver
          # (e.g., using collect() on large datasets)
          //spark.akka.frameSize=100

          # Use large object torrent broadcast to support broadcast variables larger than 2GB
          //spark.broadcast.factory= "org.apache.spark.broadcast.LargeObjectTorrentBroadcastFactory"

          # Limit of total size of serialized results of all partitions for each Spark action (e.g. collect).
          # Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit.
          # Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM).
          # Setting a proper limit can protect the driver from out-of-memory errors.
          //spark.driver.maxResultSize="1g"

          //spark.akka.retry.wait=30000
          //spark.akka.timeout=30000
          //spark.core.connection.ack.wait.timeout=600

          //spark.shuffle.consolidateFiles=true

          # Enabling RDD compression to save space (might increase CPU cycles)
          # Snappy compression is more efficient
          //spark.rdd.compress=true
          //spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec

          //spark.storage.blockManagerHeartBeatMs=300000
          //spark.storage.blockManagerSlaveTimeoutMs=300000

          //spark.worker.timeout=30000

          # To enable event logging, set spark.eventLog.enabled to true
          # and spark.eventLog.dir to the directory to which your event logs are written
          //spark.eventLog.overwrite = true
          spark.eventLog.enabled = true
          spark.eventLog.dir = "hdfs://invalid-spark-application-history-folder:8020/user/spark/applicationHistory"

          # Allow user defined functions (UDF's) to be overwritten
          spark.files.overwrite = true

          # Uncomment the following lines to enable non-standard atk i/o connectors (such as jdbc - mysql).
          # Verify that the path below exist across all yarn master/worker nodes.
          spark.driver.extraClassPath = ":/opt/cloudera/parcels/CDH/jars/mysql-connector-java-5.1.23.jar:.:"
          spark.executor.extraClassPath = ":/opt/cloudera/parcels/CDH/jars/mysql-connector-java-5.1.23.jar:postgresql-9.1-901.jdbc4.jar:"

          # Uncomment the following lines for setting extra library path (e.g. for DAAL execution in Yarn)
          // spark.driver.extraLibraryPath = "."
          // spark.executor.extraLibraryPath = "."
        }

      }
    }
  }
}
