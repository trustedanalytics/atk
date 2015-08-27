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

  engine {

    # The hdfs URL where the trustedanalytics folder will be created
    # and which will be used as the starting point for any relative URLs
    fs.root = "hdfs://invalid-fsroot-host/user/atkuser"

    # Absolute local paths where jars are copied from to hdfs-lib
    //local-libs = [ "file:/usr/lib/trustedanalytics/rest-server/lib" ]

    # The (comma separated, no spaces) Zookeeper hosts that
    # Comma separated list of host names with zookeeper role assigned
    titan.load.storage.hostname = "invalid-titan-host"

    # Titan storage backend. Available options are hbase and cassandra. The default is hbase
    //titan.load.storage.backend = "hbase"

    # Titan storage port, defaults to 2181 for HBase ZooKeeper. Use 9160 for Cassandra
    //titan.load.storage.port = "2181"

    # The URL for connecting to the Spark master server
    spark.master = "spark://invalid-spark-master:7077"

    spark.conf.properties {
      # Memory should be same or lower than what is listed as available in Cloudera Manager.
      # Values should generally be in gigabytes, e.g. "64g"
      spark.executor.memory = "invalid executor memory"
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
      file-size-to-partition-size = [{upper-bound = "1MB", partitions = 15}
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
        {upper-bound = "600GB", partitions = 3750}]

      # max-partitions is used if value is above the max upper-bound
      max-partitions = 10000

      repartition {
        # re-partitioning strategies:
        # disabled - disable re-partitioning
        # shrink_only - re-partition only when the number partitions is less than existing partitions. Uses less-expensive Spark merge
        # shrink_or_grow - re-partition can either increase or decrease the number of partitions using more-expensive Spark shuffle
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

      conf {
        properties {
          # These key/value pairs will be parsed dynamically and provided to SparkConf()
          # See Spark docs for possible values http://spark.apache.org/docs/0.9.0/configuration.html
          # All values should be convertible to Strings

          #Examples of other useful properties to edit for performance tuning:

          # Increased Akka frame size from default of 10MB to 100MB to allow tasks to send large results to Spark driver
          # (e.g., using collect() on large datasets)
          //spark.akka.frameSize=100

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
        }

      }
    }

    giraph {
      #Overrides of normal Hadoop settings that are used when running Giraph jobs
      //giraph.maxWorkers = 1
      //giraph.minWorkers = 1
      //giraph.SplitMasterWorker = true
      //mapreduce.map.memory.mb = 4096
      //mapreduce.map.java.opts = "-Xmx3072m"
      //giraph.zkIsExternal = false
    }


    titan {
      load {
        # documentation for these settings is available on Titan website
        # http://s3.thinkaurelius.com/docs/titan/current/titan-config-ref.html
        storage {

          # Whether to enable batch loading into the storage backend. Set to true for bulk loads.
          //batch-loading = true

          # Size of the batch in which mutations are persisted
          //buffer-size = 2048

          lock {
            #Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend
            //wait-time = 400

            #Number of times the system attempts to acquire a lock before giving up and throwing an exception
            //retries = 15
          }

          hbase {
            # Pre-split settngs for large datasets
            //region-count = 12
            //compression-algorithm = "SNAPPY"
          }

          cassandra {
            # Cassandra configuration options
          }
        }

        ids {
          #Globally reserve graph element IDs in chunks of this size. Setting this too low will make commits
          #frequently block on slow reservation requests. Setting it too high will result in IDs wasted when a
          #graph instance shuts down with reserved but mostly-unused blocks.
          //block-size = 300000

          #Number of partition block to allocate for placement of vertices
          //num-partitions = 10

          #The number of milliseconds that the Titan id pool manager will wait before giving up on allocating a new block of ids
          //renew-timeout = 150000

          #When true, vertices and edges are assigned IDs immediately upon creation. When false, IDs are assigned
          #only when the transaction commits. Must be disabled for graph partitioning to work.
          //flush = true

          authority {
            #This setting helps separate Titan instances sharing a single graph storage
            #backend avoid contention when reserving ID blocks, increasing overall throughput.
            # The options available are:
            #NONE = Default in Titan
            #LOCAL_MANUAL = Expert feature: user manually assigns each Titan instance a unique conflict avoidance tag in its local graph configuration
            #GLOBAL_MANUAL = User assigns a tag to each Titan instance. The tags should be globally unique for optimal performance,
            #                but duplicates will not compromise correctness
            #GLOBAL_AUTO = Titan randomly selects a tag from the space of all possible tags when performing allocations.
            //conflict-avoidance-mode = "GLOBAL_AUTO"

            #The number of milliseconds the system waits for an ID block reservation to be acknowledged by the storage backend
            //wait-time = 300

            # Number of times the system attempts ID block reservations with random conflict avoidance tags
            # before giving up and throwing an exception
            //randomized-conflict-avoidance-retries = 10
          }
        }

        auto-partitioner {
          hbase {
            # Number of regions per regionserver to set when creating Titan/HBase table
            regions-per-server = 2

            # Number of input splits for Titan reader is based on number of available cores as follows:
            #    Number of splits = Max(input-splits-per-spark-core *available spark-cores, HBase table region count),
            input-splits-per-spark-core = 2
          }

          enable = false
        }
      }

      query {
        storage {
          # query does use the batch load settings in titan.load
          backend = ${trustedanalytics.atk.engine.titan.load.storage.backend}
          hostname = ${trustedanalytics.atk.engine.titan.load.storage.hostname}
          port = ${trustedanalytics.atk.engine.titan.load.storage.port}
        }
        cache {
          # Adjust cache size parameters if you experience OutOfMemory errors during Titan queries
          # Either increase heap allocation for TrustedAnalytics Engine, or reduce db-cache-size
          # Reducing db-cache will result in cache misses and increased reads from disk
          //db-cache = true
          //db-cache-clean-wait = 20
          //db-cache-time = 180000
          #Allocates 30% of available heap to Titan (default is 50%)
          //db-cache-size = 0.3
        }
      }
    }
  }
}
