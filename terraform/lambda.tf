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

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_a_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_access" {
  name = "lambda-access"
  role = aws_iam_role.lambda_a_execution_role.id

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
        Resource = [data.aws_secretsmanager_secret.totesys_credentials.arn,
        data.aws_secretsmanager_secret.bucket_names.arn]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = ["${aws_s3_bucket.ingestion_bucket.arn}/*", "${aws_s3_bucket.lambda_bucket.arn}/*"]
      }
    ]
  })
}

data "aws_secretsmanager_secret" "totesys_credentials" {
  name = "totesys_database_credentials"
}

data "aws_secretsmanager_secret" "bucket_names" {
  name = "bucket_names"
}

# Allow logging + invoking Lambda B
# resource "aws_iam_role_policy" "lambda_a_policy" {
#   name = "lambda-a-policy"
#   role = aws_iam_role.lambda_a_execution_role.id

#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "logs:CreateLogGroup",
#           "logs:CreateLogStream",
#           "logs:PutLogEvents"
#         ]
#         Resource = "arn:aws:logs:*:*:*"
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "lambda:InvokeFunction"
#         ]
#         Resource = aws_lambda_function.lambda_b.arn
#       }
#     ]
#   })
# }

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

resource "aws_lambda_permission" "allow_lambda_a_invoke" {
  statement_id  = "AllowLambdaAInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processing_lambda.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = aws_lambda_function.ingestion_lambda.arn
}