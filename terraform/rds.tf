# VPC
resource "aws_vpc" "rds_vpc" {
  cidr_block           = "10.0.0.0/16"
#   tags = { Name = "main-vpc" }
    enable_dns_support   = true
    enable_dns_hostnames = true
    
    tags = {
        Name = "rds_vpc"
    }
}

# Private Subnets
resource "aws_subnet" "private_1" {
  vpc_id            = aws_vpc.rds_vpc.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "eu-west-2a"
  tags = { Name = "private-1" }
}

resource "aws_subnet" "private_2" {
  vpc_id            = aws_vpc.rds_vpc.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "eu-west-2b"
  tags = { Name = "private-2" }
}

# DB Subnet Group
resource "aws_db_subnet_group" "postgres_subnet_group" {
  name       = "postgres-subnet-group"
  subnet_ids = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  tags = { Name = "postgres-subnet-group" }
}

# Security Group
resource "aws_security_group" "warehouse_sg" {
  name        = "rds-postgres-sg"
  description = "Allow PostgreSQL access"
  vpc_id      = aws_vpc.rds_vpc.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    security_groups = [aws_security_group.lambda_sg.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "warehouse" {
  identifier             = "data-warehouse"
  engine                 = "postgres"
  instance_class         = "db.t3.micro"
  allocated_storage      = 25
  db_name                = "warehouse"

  username             = var.warehouse_username
  password             = random_password.warehouse.result

  vpc_security_group_ids = [aws_security_group.warehouse_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.postgres_subnet_group.name
  publicly_accessible    = false
  skip_final_snapshot    = true
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name        = "postgres-db-credentials"
  description = "RDS Postgres credentials for my app"
}

resource "aws_secretsmanager_secret_version" "db_credentials_version" {
  secret_id     = aws_secretsmanager_secret.db_credentials.id

  secret_string = jsonencode({
    username = aws_db_instance.warehouse.username
    password = random_password.warehouse.result
    host     = aws_db_instance.warehouse.address
    port     = aws_db_instance.warehouse.port
    dbname   = aws_db_instance.warehouse.db_name
    engine   = aws_db_instance.warehouse.engine
  })
}

output "db_host" {
  description = "Hostname of the RDS instance"
  value       = aws_db_instance.warehouse.address
}

# Security group for Lambda
resource "aws_security_group" "lambda_sg" {
name        = "lambda-sg"
  description = "Security group for Lambda"
  vpc_id = aws_vpc.rds_vpc.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks              = ["0.0.0.0/0"]  # Only needed for interface endpoint outbound
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks              = ["0.0.0.0/0"]  # Only needed for interface endpoint outbound
  }
}

# Route tables group for the subnets
resource "aws_route_table" "private_1" {
  vpc_id = aws_vpc.rds_vpc.id
}

resource "aws_route_table_association" "private_a_assoc" {
  subnet_id      = aws_subnet.private_1.id
  route_table_id = aws_route_table.private_1.id
}

resource "aws_route_table" "private_2" {
  vpc_id = aws_vpc.rds_vpc.id
}

resource "aws_route_table_association" "private_b_assoc" {
  subnet_id      = aws_subnet.private_2.id
  route_table_id = aws_route_table.private_2.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.rds_vpc.id
  service_name      = "com.amazonaws.eu-west-2.s3"
  vpc_endpoint_type = "Gateway"

  route_table_ids = [
  aws_route_table.private_1.id,
  aws_route_table.private_2.id
]
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id            = aws_vpc.rds_vpc.id
  service_name      = "com.amazonaws.eu-west-2.secretsmanager"
  vpc_endpoint_type = "Interface"
  private_dns_enabled = true

  # Subnets where the Lambda is deployed
  subnet_ids = [
    aws_subnet.private_1.id,
    aws_subnet.private_2.id
  ]

  # Security group allowing Lambda to reach the endpoint
  security_group_ids = [
    aws_security_group.lambda_sg.id
  ]
}