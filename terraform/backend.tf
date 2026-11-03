terraform {
  backend "s3" {
    bucket         = "ray-platform-tfstate"
    key            = "dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "ray-platform-tflocks"
  }
}
