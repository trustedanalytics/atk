applications:
- name:  atk-xxx # App name
  command: bin/rest-server.sh
  memory: 1G
  disk_quota: 1G
  timeout: 180
  instances: 1
  domain: 52.88.248.77.xip.io
services:
- hbase-xxx
- hdfs-xxx
- yarn-xxx
- mysql56-xxx
- zookeeper-xxx
- pg93-xxx
env:
 CC_URI: api.52.88.248.77.xip.io
 UAA_URI: uaa.52.88.248.77.xip.io
 UAA_CLIENT_NAME: atk-client
 UAA_CLIENT_PASSWORD: taste-coward-xxxx-xxxx
