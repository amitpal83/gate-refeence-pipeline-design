output "platform_instance_id" {
  value       = aws_instance.platform.id
  description = "Platform EC2 instance ID."
}

output "platform_public_ip" {
  value       = aws_instance.platform.public_ip
  description = "Public IP of the demo host — the demo UIs are reachable directly at http://<this-ip>:<port> from allowed_demo_cidr."
}

output "demo_ui_urls" {
  description = "Direct browser URLs for the team demo — reachable from allowed_demo_cidr, no SSH tunnel needed."
  value = {
    airflow  = "http://${aws_instance.platform.public_ip}:8080"
    trino    = "http://${aws_instance.platform.public_ip}:8081"
    kafka_ui = "http://${aws_instance.platform.public_ip}:8082"
    minio    = "http://${aws_instance.platform.public_ip}:9001"
    datahub  = "http://${aws_instance.platform.public_ip}:9002"
  }
}

output "ssm_session_command" {
  description = "Shell access via AWS Systems Manager — no SSH, no IP-based security-group rule, gated by your AWS IAM permissions instead. Requires the Session Manager plugin installed locally."
  value       = "aws ssm start-session --target ${aws_instance.platform.id} --region ${var.aws_region}"
}

output "backup_bucket" {
  value = var.backup_bucket_name == "" ? null : aws_s3_bucket.backup[0].bucket
}