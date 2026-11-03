output "cluster_name" {
  value = aws_eks_cluster.main.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.main.endpoint
}

output "ray_irsa_role_arn" {
  value = aws_iam_role.ray_irsa.arn
}

output "s3_bucket_name" {
  value = aws_s3_bucket.iceberg.bucket
}
