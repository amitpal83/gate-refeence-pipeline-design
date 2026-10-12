# OpenBao server configuration — POC, single node.

ui            = true
# Raft + containers: mlock is not available, so it is disabled. IPC_LOCK is
# still granted in docker-compose.geo.yml for when it is.
disable_mlock = true

storage "raft" {
  path    = "/openbao/data"
  node_id = "gates-bao-1"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  # POC: plain HTTP, reachable only on the compose network and on loopback.
  # Production requires TLS (mTLS between services).
  tls_disable = true
}

api_addr     = "http://openbao:8200"
cluster_addr = "http://openbao:8201"
