resource "aws_lambda_layer_version" "lambda_layer" {
  layer_name          = "lambda-layer"
  s3_bucket = aws_s3_bucket.lambda_bucket.bucket
  s3_key =  aws_s3_object.lambda_layer.key
  compatible_runtimes = ["python3.13"]
  description         = "Shared utilities layer"
}

data "archive_file" "lambda_ingestion_zip" {
  type        = "zip"
  source_file  = "${path.module}/../src/lambda_ingestion.py"
  output_path = "${path.module}/lambdas/lambda_ingestion.zip"
}

resource "aws_lambda_function" "ingestion_lambda" {
  function_name = "ingestion-lambda"
  role          = aws_iam_role.lambda_a_execution_role.arn
  handler       = "lambda_ingestion.lambda_ingestion"
  runtime       = "python3.13"
  filename      = data.archive_file.lambda_ingestion_zip.output_path

  source_code_hash = filebase64sha256(data.archive_file.lambda_ingestion_zip.output_path)


  environment {
    variables = {
      LOG_LEVEL = "INFO"
    }
  }
  layers = [
    aws_lambda_layer_version.lambda_layer.arn
  ]

  # ✅ Increase timeout to 60 seconds (default is 3 seconds)
  timeout = 60

  # Optional: memory allocation
  memory_size = 512
}

data "archive_file" "lambda_processing_zip" {
  type        = "zip"
  source_file  = "${path.module}/../src/lambda_processing.py"
  output_path = "${path.module}/lambdas/lambda_processing.zip"
}

resource "aws_lambda_function" "processing_lambda" {
  function_name = "processing-lambda"
  role          = aws_iam_role.lambda_b_execution_role.arn
  handler       = "lambda_processing.lambda_processing"
  runtime       = "python3.13"
  filename      = data.archive_file.lambda_processing_zip.output_path

  source_code_hash = filebase64sha256(data.archive_file.lambda_processing_zip.output_path)

  environment {
    variables = {
      LOG_LEVEL = "INFO"
    }
  }
  layers = [
    aws_lambda_layer_version.lambda_layer.arn

  ]
  # ✅ Increase timeout to 60 seconds (default is 3 seconds)
  timeout = 60

  # Optional: memory allocation
  memory_size = 512
}

resource "aws_iam_role" "lambda_a_execution_role" {
  name = "lambda-a-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
#   role       = aws_iam_role.lambda_a_execution_role.name
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
# }

resource "aws_iam_policy" "lambda_access" {
  name = "lambda-access"
  # role = aws_iam_role.lambda_a_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        # Resource = ["arn:aws:secretsmanager:eu-west-2:682059290491:secret:totesys_database_credentials-60CQzv",
        # "arn:aws:secretsmanager:eu-west-2:682059290491:secret:bucket_names-LGVI0a"]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = ["${aws_s3_bucket.ingestion_bucket.arn}/*", "${aws_s3_bucket.lambda_bucket.arn}/*", "${aws_s3_bucket.processed_bucket.arn}/*"]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "warehousing_vpc_access" {
  name = "lambda-vpc-access"
  role = aws_iam_role.lambda_warehouse_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_access_attach" {
  for_each   = {
    lambda1 = aws_iam_role.lambda_a_execution_role.name
    lambda2 = aws_iam_role.lambda_b_execution_role.name
    lambda3 = aws_iam_role.lambda_warehouse_execution_role.name
  }

  role       = each.value
  policy_arn = aws_iam_policy.lambda_access.arn
}

# data "aws_secretsmanager_secret" "totesys_credentials" {
#   name = "totesys_database_credentials"
# }

# data "aws_secretsmanager_secret" "bucket_names" {
#   name = "bucket_names"
# }

# data "aws_secretsmanager_secret" "db_credentials" {
#   name        = "postgres-db-credentials"
# }

resource "aws_iam_role_policy" "lambda_a_invoke_policy" {
  name = "lambda-a-invoke-policy"
  role = aws_iam_role.lambda_a_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.processing_lambda.arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_warehouse_invoke_policy" {
  name = "lambda-warehouse-invoke-policy"
  role = aws_iam_role.lambda_b_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.warehouse_lambda.arn
      }
    ]
  })
}

resource "aws_iam_role" "lambda_b_execution_role" {
  name = "lambda-b-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

data "archive_file" "lambda_warehousing_zip" {
  type        = "zip"
  source_file  = "${path.module}/../src/lambda_warehousing.py"
  output_path = "${path.module}/lambdas/lambda_warehousing.zip"
}

resource "aws_iam_role" "lambda_warehouse_execution_role" {
    name = "lambda-warehousing-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_lambda_function" "warehouse_lambda" {
  function_name = "warehouse-lambda"
  role          = aws_iam_role.lambda_warehouse_execution_role.arn
  handler       = "lambda_warehousing.lambda_warehousing"
  runtime       = "python3.13"
  filename      = data.archive_file.lambda_warehousing_zip.output_path

  source_code_hash = filebase64sha256(data.archive_file.lambda_warehousing_zip.output_path)

  environment {
    variables = {
      LOG_LEVEL = "INFO"
    }
  }
  layers = [
    aws_lambda_layer_version.lambda_layer.arn

  ]

  vpc_config {
        subnet_ids = [
            aws_subnet.private_1.id, 
            aws_subnet.private_2.id
        ]
        security_group_ids = [aws_security_group.lambda_sg.id]
  }
  # ✅ Increase timeout to 60 seconds (default is 3 seconds)
  timeout = 60

  # Optional: memory allocation
  memory_size = 512
}

# resource "aws_lambda_permission" "allow_lambda_a_invoke" {
#   statement_id  = "AllowLambdaAInvoke"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.processing_lambda.function_name
#   principal     = "lambda.amazonaws.com"
#   source_arn    = aws_lambda_function.ingestion_lambda.arn
# }