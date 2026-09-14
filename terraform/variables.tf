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
  description = "Your public IP in CIDR notation, for example 203.0.113.10/32."
  type        = string
}

variable "allowed_demo_cidr" {
  description = <<-EOT
    CIDR range allowed direct browser access to the demo UIs (Airflow 8080,
    Trino 8081, Kafka UI 8082, MinIO console 9001, DataHub frontend 9002),
    so the team can watch the pipeline run without each person tunneling
    over SSH. Scope this to the presenter's IP or the venue/office network
    range for the demo window — never 0.0.0.0/0. Defaults to
    allowed_ssh_cidr if not set separately.
  EOT
  type        = string
  default     = ""
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
  description = "Existing EC2 key pair name."
  type        = string
}