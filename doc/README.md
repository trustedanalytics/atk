Doc Generation
==============

The various steps required to generate the sphinx’s based documentation are run through the exec-maven-plugin.

To generate only the documentation run
```
mvn package –pl doc –P doc
```
This will generate and run only the documentation module.

If you just pulled new changes or made changes to any code run
```
mvn install -P doc -DskipTests
```
This will generate the docs and package all the changes so doc generation can pick them up.

Doc generation currently requires 7 steps
  1. Copies [local mode configuration](../conf/examples/applicatiion.conf.build) to conf/
  2. start rest server
  3. Compile trustedanalytics python client
  4. run [build_docs.py](../python-client/trustedanalytics/doc/build_docs.py)
  5. run sphinx-build
  6. stop rest server
  7. archive documentation 


