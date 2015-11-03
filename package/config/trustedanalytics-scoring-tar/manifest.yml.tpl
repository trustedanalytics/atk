applications:
- name: SE # App name
  command: bin/scoring-server.sh
  memory: 1G
  disk_quota: 1G
  timeout: 180
  instances: 1
services:
- hdfs-atk # hdfs service which holds the model tar file