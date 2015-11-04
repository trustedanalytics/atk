applications:
- name: SE # App name
  command: bin/scoring-server.sh
  memory: 1G
  disk_quota: 1G
  timeout: 180
  instances: 1
services:
- hdfs-atk # hdfs service which holds the model tar file
env:
TAR_ARCHIVE: 'hdfs://nameservice1/user/atkuser/testATK/models_05f7d49ef1c246fa9b99bf417bd3ebe6.tar' # Model tarball location
