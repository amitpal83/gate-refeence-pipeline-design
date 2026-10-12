# Production security for the geo pipeline

The proof of concept runs on one EC2 host with MinIO. Production will use OpenBao and Ceph. The PII design is the same in both: Presidio, the `pii:` policy, OpenBao Transit and the vault do not depend on the storage layer. This page lists each shortcut taken in the PoC and what replaces it.

Items marked **verify** are assumptions that have not been tested against your versions. Test them early.

## PoC shortcuts and their production replacements

| Area | PoC today | Production |
|---|---|---|
| Key service availability | One OpenBao node | HA Raft cluster of 3 or 5 nodes. If OpenBao is down, the pipeline cannot protect data and fails closed, and Ceph cannot fetch keys for encrypted objects |
| Unseal | One key share, stored on the host in `keys.env` | Auto-unseal from a second OpenBao or an HSM, or Shamir shares held by different people. Never a key beside the data it protects |
| Credential delivery | AppRole files on a shared folder, one world-readable | OpenBao agent that logs in, renews the token and delivers it. No long-lived secret in a file |
| Root token | In `keys.env`, used once by `scripts/geo_setup.py` | Revoke it after setup. Use named admin policies |
| In transit | Plain HTTP between services | TLS on OpenBao, PostGIS, GeoServer and the object store, with mTLS between services. Ceph messenger v2 in `secure` mode inside the cluster |
| Object store | MinIO | Ceph RGW (S3 API) |
| Object encryption | None on the MinIO data beyond the encrypted disk | RGW server-side encryption with keys from OpenBao. **verify** |
| Disk encryption | Encrypted EBS volumes (AWS-managed key) | Ceph OSD encryption (dmcrypt/LUKS) |
| Storage credentials | MinIO root user, stored in `kv/geo/minio` | A scoped user per service, with a policy limited to its buckets |
| Vault location | `pii_vault` schema in the same PostGIS database | A separate database or host, with its own network rule |
| PostGIS data | Encrypted EBS volume | Encrypted volume or RBD with LUKS, key in OpenBao. Object-store encryption does not cover it |
| Quarantine | MinIO bucket, objects expire after 14 days (configurable) | Keep a lifecycle rule with the agreed number of days, add encryption, and limit access to the pipeline role |
| GeoServer access | One admin account | Named viewer accounts with read-only roles. Keep anonymous access closed. Keep the port off the internet |
| Audit log | A file inside the OpenBao container volume | Ship to central logging, outside the key service's own host |
| Source folder | Read-only mount | A writable drop location, with processed files moved out |
| Working files | `/tmp/geo-work` in one container | A protected shared volume if there is more than one worker, or keep one worker |

## Verify early: Ceph RGW with OpenBao

RGW documents encryption with HashiCorp Vault. OpenBao is a fork with a compatible API, so it should work, but this has not been tested. RGW's supported authentication methods are narrower than OpenBao's. Test it on a small Ceph cluster before relying on it:

1. Put an object in a bucket with default encryption set to a key held in OpenBao.
2. Read the object straight off the OSD. It must be unreadable.
3. Revoke or disable the key. The object must become unreadable through RGW too.
4. Check multipart uploads and the Trino and GeoPandas access paths against RGW, since SSE behaviour can differ slightly from MinIO.

The same test applies to MinIO with KES in the PoC. That path is not set up yet. If KES does not accept OpenBao in your versions, fall back to MinIO SSE-S3 for the PoC and record the gap.

Token handling matters: RGW needs a long-lived credential to call OpenBao. Use an OpenBao agent to renew it. A leaked token can decrypt data keys.

## Keys

| Key | Use | Rotation |
|---|---|---|
| `pii-hmac` | Pseudonyms. Deterministic, so the same person has the same token everywhere | Rotating changes every new pseudonym, so old and new tokens will not match. Decide first whether to re-key history. The key version is inside each token (`vault:v1:...`) |
| `pii-enc` | Vault encryption | Rotate on a schedule with `transit/keys/pii-enc/rotate`. Move old rows to the new version without exposing plaintext using `rewrap`. Raise `min_decryption_version` once nothing uses the old versions |

Back up OpenBao's storage and its unseal material separately, and in different places. If both are lost, every vault value is lost permanently.

## Personal data

- Pseudonymized and encrypted data is still personal data under the Philippine Data Privacy Act, because anyone holding the key can re-link it. Treat the vault as the most sensitive store in the platform.
- Using the vault's contact details to send messages is a new purpose. It needs a lawful basis such as consent, a record of that consent, and an opt-out. Add a consent flag per contact before any application reads the vault.
- Presidio reduces risk and does not guarantee detection, especially for Filipino names in free text. Review the findings counts for every new layer, tune the threshold, and add recognizers or deny-lists where it misses.
- Person-level locations (for example a household survey point) are personal data in themselves. Layers like that need a rule to coarsen or aggregate the geometry before storage. Facility points are public infrastructure and stay precise.
- Set a retention period for the layer data, the vault rows and the quarantine bucket, and a way to delete a person's vault rows on request.
- If the agency has requirements on data residency or approved algorithms, they override the defaults here.

## Access

| Who | May | May not |
|---|---|---|
| Pipeline tasks (`geo-loader`, `geo_loader`) | Protect, encrypt, write the serving data and the vault | Decrypt. Read vault ciphertext |
| Map users (`geoserver_ro`) | Read the serving view | Read the base table or the vault |
| Notification application (`pii-reader`, `vault_reader`) | Read vault ciphertext and decrypt it, per record, with an audit entry | Write data. Use any other key |
| Platform administrators | Operate the services | Read decrypted contact details without using the reader role, so that it shows in the audit log |

## Order of work for production

1. Test Ceph RGW with OpenBao (above).
2. Stand up OpenBao as an HA cluster with auto-unseal. Move credentials to the agent.
3. Turn on TLS everywhere.
4. Move the vault to its own database or host.
5. Replace root and shared credentials with scoped ones. Revoke the root token.
6. Set retention for quarantine, layers and the vault. Add the consent record.
7. Ship the audit log off the host.
