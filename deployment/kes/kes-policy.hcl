# kes: the OpenBao policy for KES, which keeps the master keys for the geo MinIO.
# KES stores each master key as a kv v2 secret under kv/kes/<key name>. It can
# create, read and delete those and nothing else. The geo-loader policy cannot
# read this path.

path "kv/data/kes/*" {
  capabilities = ["create", "read", "update", "delete"]
}

path "kv/metadata/kes/*" {
  capabilities = ["list", "read", "delete"]
}
