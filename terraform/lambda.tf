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

  environment {
    variables = {
      LOG_LEVEL = "INFO"
    }
  }
  layers = [
    aws_lambda_layer_version.lambda_layer.arn
  ]
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

  environment {
    variables = {
      LOG_LEVEL = "INFO"
    }
  }
  layers = [
    aws_lambda_layer_version.lambda_layer.arn

  ]
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