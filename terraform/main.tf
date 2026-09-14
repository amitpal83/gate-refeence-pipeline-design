locals {
  demo_cidr = var.allowed_demo_cidr != "" ? var.allowed_demo_cidr : var.allowed_ssh_cidr
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "demo" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.demo.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, 1)
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "demo" {
  vpc_id = aws_vpc.demo.id
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.demo.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.demo.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "platform" {
  name        = "${var.project_name}-platform"
  description = "Private GATES services with SSH tunnel access."
  vpc_id      = aws_vpc.demo.id

  ingress {
    description = "SSH administration"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  ingress {
    description = "Internal service traffic"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  # Direct browser access to the demo UIs — Airflow, Trino, Kafka UI, MinIO
  # console, DataHub frontend — for the team demo, so no one needs an SSH
  # tunnel just to watch the pipeline run. Scoped to allowed_demo_cidr
  # (defaults to allowed_ssh_cidr), never open to 0.0.0.0/0. Each service
  # still sits behind its own login. Tighten this back down (or
  # terraform destroy) once the demo is over.
  dynamic "ingress" {
    for_each = toset([8080, 8081, 8082, 9001, 9002])
    content {
      description = "Demo UI port ${ingress.value}"
      from_port   = ingress.value
      to_port     = ingress.value
      protocol    = "tcp"
      cidr_blocks = [local.demo_cidr]
    }
  }

  egress {
    description = "Outbound service access"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "platform" {
  name = "${var.project_name}-platform-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent" {
  role       = aws_iam_role.platform.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.platform.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "platform" {
  name = "${var.project_name}-platform-profile"
  role = aws_iam_role.platform.name
}

resource "aws_ebs_volume" "data" {
  availability_zone = aws_subnet.public.availability_zone
  size              = var.data_volume_size_gb
  type              = "gp3"
  encrypted         = true
}

resource "aws_instance" "platform" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.platform.id]
  key_name                    = var.ssh_key_name
  iam_instance_profile        = aws_iam_instance_profile.platform.name
  associate_public_ip_address = true
  user_data_replace_on_change = true

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_size_gb
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    project_name = var.project_name
  })

  tags = {
    Name = "${var.project_name}-platform"
  }
}

resource "aws_volume_attachment" "data" {
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.platform.id
}

resource "aws_cloudwatch_log_group" "platform" {
  name              = "/gates/${var.environment}/platform"
  retention_in_days = 30
}

resource "aws_s3_bucket" "backup" {
  count  = var.backup_bucket_name == "" ? 0 : 1
  bucket = var.backup_bucket_name
}

resource "aws_s3_bucket_public_access_block" "backup" {
  count                   = var.backup_bucket_name == "" ? 0 : 1
  bucket                  = aws_s3_bucket.backup[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}