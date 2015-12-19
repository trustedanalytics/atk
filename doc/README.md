Doc Generation
==============

The various steps required to generate the sphinx’s based documentation are run through the exec-maven-plugin.

To generate only the documentation run
```
mvn package –P doc -pl doc 
```


If you just pulled new changes or made changes to any code run
```
mvn install -P doc -DskipTests
```


Doc generation currently requires 7 steps
  1. Copies [local mode configuration](../conf/examples/application.conf.build) to conf/
  2. start rest server
  3. Compile trustedanalytics python client
  4. run [build_docs.py](../python-client/trustedanalytics/doc/build_docs.py)
  5. run sphinx-build
  6. stop rest server
  7. archive documentation 


