
This folder is for integration tests that are run at build time.

- Runs out of source code and assumes jars have been built using 'mvn package'
- Tests are ran in parallel
- Spark is ran in local mode - one context is shared between all tests
- No HDFS, everything is against local file system
- No Titan available, yet (maybe Berkeley later)
- No PostgreSQL, all tests are against in-memory H2 DB
- All output should go to 'target' folder and using in-memory DB - so no clean-up is needed in tests
- You can view the data left behind from a test

- TODO: Allow tests to go against Titan
- TODO: Modify logging to go under target/logs (/var/logs gets some still)
- TODO: We should start on random port since it doesn't seem to get released right away (or we need a better test to see if it is still open)
- TODO: Python UDF's aren't working (error message about can't find pyspark)

-------------------
  Files
-------------------
/integration-tests
    /datasets - folder for data sets, these will be copied to fs root
    /target - folder for all generated files, fs root gets created under here
        /fs-root - folder that functions like the HDFS root used by our application
            /datasets - copy of top-level /datasets folder
            /trustedanalytics - frames and graphs get written here
        /rest-server.log - output from REST Server
        /rest-server.pid - contains PID of REST Server so we can kill it when we're done
    /smoketests - pyunit tests that are run first, these only verify the basic functionality of the system needed by all of the other tests
    /tests - pyunit tests, most tests go here
    /rest-server-start.sh - script for starting REST server (you don't need to call directly)
    /rest-server-stop.sh - script for stopping REST server (that was started with the start script)
    /clean.sh - script for removing generated files (removes everything under target folder)
    /README.txt - this text file
    /run_tests.sh - script for running tests (start server, run tests, stop server)

-------------------
  Prerequisites
-------------------
The following python packages are required:
  sudo pip2.7 install nose             # version >= 1.3.4
  sudo pip2.7 install nose_xunitmp     # version > 0.2


  Instructions
-------------------
- Please only add the absolutely smallest toy data sets!
- Remember these are build-time tests, we need to have them run as fast as possible
- Avoid outside dependencies (Spark, HDFS, etc.), these tests should be self-contained so they can easily run on any build server
- Tests should be short, isolated, and fast running.
- Preferably we treat data sets as immutable
