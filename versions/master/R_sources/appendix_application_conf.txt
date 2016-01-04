.. _appendix_application_conf:

=====================================================
Appendix A |EM| Sample Application Configuration File
=====================================================
.. code::

    # BEGIN REQUIRED SETTINGS

    trustedanalytics.atk {
    #bind address - change to 0.0.0.0 to listen on all interfaces
    //api.host = "127.0.0.1"

    #bind port
    //api.port = 9099

    # The host name for the Postgresql database in which the metadata will be stored
    metastore.connection-postgresql.host = "localhost"
    metastore.connection-postgresql.port = "5432"
    metastore.connection-postgresql.database = "ta_metastore"
    metastore.connection-postgresql.username = "atkuser"
    metastore.connection-postgresql.password = "MyPassword"
    metastore.connection-postgresql.url = 
        "jdbc:postgresql://"${trustedanalytics.atk.metastore.connection-postgresql.host}":
        "${trustedanalytics.atk.metastore.connection-postgresql.port}"/
        "${trustedanalytics.atk.metastore.connection-postgresql.database}

    # This allows for the use of postgres for a metastore.
    # Service restarts will not affect the data stored in postgres.
    metastore.connection = ${trustedanalytics.atk.metastore.connection-postgresql}

    # This allows the use of an in memory data store.
    # Restarting the REST server will create a fresh database and any
    # data in the h2 DB will be lost
    //metastore.connection = ${trustedanalytics.atk.metastore.connection-h2}

    engine {

        # The hdfs URL where the trustedanalytics folder will be created
        # and which will be used as the starting point for any relative URLs
        fs.root = "hdfs://master.silvern.gao.cluster:8020/user/atkuser"


        # The URL for connecting to the Spark master server
        #spark.master = "spark://master.silvern.gao.cluster:7077"
        yarn-client = "spark://master.silvern.gao.cluster:7077"


        spark.conf.properties {
            # Memory should be same or lower than what is listed as available
            # in Cloudera Manager.
            # Values should generally be in gigabytes, e.g. "64g".
            spark.executor.memory = "103079215104"
        }
    }

    }
    # END REQUIRED SETTINGS

    # The settings below are all optional.
    # Some may need to be configured depending on the
    # specifics of your cluster and workload.

    trustedanalytics.atk {
      engine {
        auto-partitioner {
          # auto-partitioning spark based on the file size
          file-size-to-partition-size = [
                                           { upper-bound="1MB", partitions = 15 },
                                           { upper-bound="1GB", partitions = 45 },
                                           { upper-bound="5GB", partitions = 100 },
                                           { upper-bound="10GB", partitions = 200 },
                                           { upper-bound="15GB", partitions = 375 },
                                           { upper-bound="25GB", partitions = 500 },
                                           { upper-bound="50GB", partitions = 750 },
                                           { upper-bound="100GB", partitions = 1000 },
                                           { upper-bound="200GB", partitions = 1500 },
                                           { upper-bound="300GB", partitions = 2000 },
                                           { upper-bound="400GB", partitions = 2500 },
                                           { upper-bound="600GB", partitions = 3750 }
                                        ]
      # max-partitions is used if value is above the max upper-bound
              max-partitions = 10000
          }
        }

        # Configuration for the Trusted Analytics ATK REST API server
        api {
          # this is reported by the API server in the /info results -
          # it can be used to identify a particular server or cluster.
          //identifier = "ta"

          #The default page size for result pagination
          //default-count = 20

          #Timeout for waiting for results from the engine
          //default-timeout = 30s

          #HTTP request timeout for the REST server
          //request-timeout = 29s
        }

          #Configuration for the processing engine
          engine {
              //default-timeout = 30s
             //page-size = 1000

        spark {

          # When master is empty the system defaults to spark://`hostname`:7070
          # where hostname is calculated from the current system.
          # For local mode (useful only for development testing) set master = "local[4]"
          # in cluster mode, set master and home like the example
          # master = "spark://MASTER_HOSTNAME:7077"
          # home = "/opt/cloudera/parcels/CDH/lib/spark"

          # When home is empty the system will check expected locations on the
          # local system and use the first one it finds.
          # If spark is running in yarn-cluster mode (spark.master = "yarn-cluster"),
          # spark.home needs to be set to the spark directory on CDH cluster
          # ("/usr/lib/spark","/opt/cloudera/parcels/CDH/lib/spark/", etc)
          //home = ""

          conf {
            properties {
              # These key/value pairs will be parsed dynamically and provided
              # to SparkConf().
              # See Spark docs for possible values
              # http://spark.apache.org/docs/0.9.0/configuration.html.
              # All values should be convertible to Strings.

              #Examples of other useful properties to edit for performance tuning:

              # Increased Akka frame size from default of 10MB to 100MB to
              # allow tasks to send large results to Spark driver
              # (e.g., using collect() on large datasets).
              //spark.akka.frameSize=100

              #spark.akka.retry.wait=30000
              #spark.akka.timeout=200
              #spark.akka.timeout=30000

              //spark.shuffle.consolidateFiles=true

              # Enabling RDD compression to save space (might increase CPU cycles)
              # Snappy compression is more efficient
              //spark.rdd.compress=true
              //spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec

              #spark.storage.blockManagerHeartBeatMs=300000
              #spark.storage.blockManagerSlaveTimeoutMs=300000

              #spark.worker.timeout=600
              #spark.worker.timeout=30000
              spark.eventLog.enabled=true
              spark.eventLog.dir=
              "hdfs://master.silvern.gao.cluster:8020/user/spark/applicationHistory"
            }

          }
        }
      }
    }

