echo "[trustedanalytics-deps]
name=trustedanalytics-deps
baseurl=https://trustedanalytics-dependencies.s3-us-west-2.amazonaws.com/yum
gpgcheck=0
priority=1 enabled=1"  | sudo tee -a /etc/yum.repos.d/ta-deps.repo

