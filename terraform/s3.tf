resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-joe-ingestion-bucket-2025"

  tags = {
    Name        = "ingestion-bucket"
  }
}

resource "aws_s3_bucket" "processed_bucket" {
    bucket = "nc-joe-processed-bucket-2025"

    tags = {
        Name = "processed-bucket"
    }
}

resource "aws_s3_bucket" "lambda_bucket" {
    bucket = "nc-lambda-bucket-joe-final-project-2025"

    tags = {
        Name = "lambda-bucket"
    }
}

data "archive_file" "layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../python"   # folder containing your dependencies
  output_path = "${path.module}/layers/layer.zip"
}

resource "aws_s3_object" "lambda_layer" {
  bucket = "nc-lambda-bucket-joe-final-project-2025"
  key    = "layers/lambda_layer.zip"
  source = data.archive_file.layer_zip.output_path
}