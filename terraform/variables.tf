variable "aws_region" {
  description = "AWS region for the demo deployment."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short project name used in resource names."
  type        = string
  default     = "gates-demo"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "demo"
}

variable "vpc_cidr" {
  description = "CIDR range for the demo VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "allowed_ssh_cidr" {
  description = <<-EOT
    TEMPORARY debugging fallback — re-added because SSM Session Manager
    wasn't registering on the first launch attempt and there was no way to
    diagnose why without a shell. Set to "" to remove the SSH ingress rule
    again entirely (recommended once SSM is confirmed working) — leaving it
    set keeps port 22 open to this CIDR indefinitely.
  EOT
  type        = string
  default     = ""
}

variable "allowed_demo_cidr" {
  description = <<-EOT
    CIDR range allowed direct browser access to the demo UIs (Airflow 8080,
    Trino 8081, Kafka UI 8082, MinIO console 9001, DataHub frontend 9002).
    Set to "0.0.0.0/0" to make these UIs reachable from anywhere on the
    internet — deliberately chosen for this deployment so remote viewers
    don't need a CIDR-scoped range. These are demo-grade services with
    simple credentials, not hardened for open-ended internet exposure, so
    tighten this back down (or terraform destroy) once the demo is over.
  EOT
  type        = string

  validation {
    condition     = var.allowed_demo_cidr != ""
    error_message = "allowed_demo_cidr must be set — use \"0.0.0.0/0\" for open internet access, or scope it to a specific CIDR."
  }
}

variable "instance_type" {
  description = <<-EOT
    Single-node size running Airflow, Kafka, Kafka Connect, source-db, Trino,
    MinIO, Nessie, DataHub (+ its Elasticsearch/MySQL/frontend), and the
    config Postgres, all at once via Docker Compose. m6i.2xlarge (8 vCPU /
    32 GB) relies on the per-service mem_limit caps in
    docker-compose.demo.yml to fit reliably — those caps sum to ~19 GB
    across all 19 services, leaving headroom for the host OS and Docker
    itself. Without those caps, several JVM-based services size their
    default heap off total visible host memory and can oversubscribe a
    32 GB box; don't raise these limits materially without either bumping
    this instance size too or re-checking the total against available RAM.
  EOT
  type        = string
  default     = "m6i.2xlarge"
}

variable "root_volume_size_gb" {
  description = "Root volume size for the platform node."
  type        = number
  default     = 80
}

variable "data_volume_size_gb" {
  description = "Persistent EBS volume for MinIO, Kafka, and service data."
  type        = number
  default     = 400
}

variable "backup_bucket_name" {
  description = "Optional globally unique S3 bucket name for backups."
  type        = string
  default     = ""
}

variable "ssh_key_name" {
  description = <<-EOT
    Optional existing EC2 key pair name. Not required — admin access goes
    through SSM Session Manager (no SSH ingress rule exists at all), so
    leave this "" unless you specifically want a key pair attached as a
    fallback (it has no effect unless you also open port 22 yourself).
  EOT
  type        = string
  default     = ""
}