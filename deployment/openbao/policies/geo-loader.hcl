# geo-loader: used by pipeline tasks that PROTECT data on the way in.
# It can pseudonymize and encrypt. It can NOT decrypt, rewrap or read keys.

path "transit/hmac/pii-hmac" {
  capabilities = ["update"]
}

path "transit/encrypt/pii-enc" {
  capabilities = ["update"]
}

# Service credentials for the geo stores (PostGIS, GeoServer, MinIO).
path "kv/data/geo/*" {
  capabilities = ["read"]
}
