# pii-reader: used by applications that are allowed to recover original
# contact details (for example to send emails or messages). Every call is
# written to the OpenBao audit log. It can decrypt pii-enc and nothing else.

path "transit/decrypt/pii-enc" {
  capabilities = ["update"]
}
