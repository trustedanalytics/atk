# Archive declaration
trustedanalytics.atk.component.archives {
  scoring-engine {
    parent = "scoring-interfaces"
    class = "org.trustedanalytics.atk.scoring.ScoringServiceApplication"
   }
}

trustedanalytics.scoring-engine {
#name of the tar file that contains the model implementation, model class name and model bytes
  archive-tar = "hdfs://SOME_HOST:8020/user/atkuser/test.tar"
}

trustedanalytics.atk {
  scoring {
    identifier = "ia"
    #bind address - change to 0.0.0.0 to listen on all interfaces
    host = "127.0.0.1"
    port = 9100
    default-count = 20
    default-timeout = 30s
    request-timeout = 29s
    logging {
      raw = false
      profile = false
    }
  }
}



