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

output "ssh_tunnel_command" {
  description = "Fallback for anyone outside allowed_demo_cidr, or for solo debugging."
  value = "ssh -i <key.pem> -L 8080:localhost:8080 -L 9002:localhost:9002 -L 8081:localhost:8081 -L 9001:localhost:9001 -L 8082:localhost:8082 ec2-user@${aws_instance.platform.public_ip}"
}

output "backup_bucket" {
  value = var.backup_bucket_name == "" ? null : aws_s3_bucket.backup[0].bucket
}