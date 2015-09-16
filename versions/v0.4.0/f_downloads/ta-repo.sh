echo "[trustedanalytics]
name= trusted analytics
baseurl=https://trustedanalytics-repo.s3-us-west-2.amazonaws.com/release/latest/yum/dists/rhel/6
gpgcheck=0
priority=1
s3_enabled=1
key_id=ACCESS_TOKEN
secret_key=SECRET_TOKEN" | sudo tee -a /etc/yum.repos.d/trustedanalytics.repo


