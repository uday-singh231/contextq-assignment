data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

# -------------------------------------------------------------
# 1. NETWORKING (VPC, Subnets, Gateways, Routes)
# -------------------------------------------------------------
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.environment}-ray-vpc"
  }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.main.id
}

resource "aws_subnet" "public_1" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = {
    Name                     = "${var.environment}-public-1"
    "kubernetes.io/role/elb" = "1"
  }
}

resource "aws_subnet" "public_2" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = data.aws_availability_zones.available.names[1]
  map_public_ip_on_launch = true

  tags = {
    Name                     = "${var.environment}-public-2"
    "kubernetes.io/role/elb" = "1"
  }
}

resource "aws_subnet" "private_1" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.10.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name                              = "${var.environment}-private-1"
    "kubernetes.io/role/internal-elb" = "1"
  }
}

resource "aws_subnet" "private_2" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.11.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name                              = "${var.environment}-private-2"
    "kubernetes.io/role/internal-elb" = "1"
  }
}

resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public_1.id
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
}

resource "aws_route_table_association" "pub_1" {
  subnet_id      = aws_subnet.public_1.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "pub_2" {
  subnet_id      = aws_subnet.public_2.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }
}

resource "aws_route_table_association" "priv_1" {
  subnet_id      = aws_subnet.private_1.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "priv_2" {
  subnet_id      = aws_subnet.private_2.id
  route_table_id = aws_route_table.private.id
}

# -------------------------------------------------------------
# 2. S3 STORAGE & GLUE CATALOG (Apache Iceberg)
# -------------------------------------------------------------
resource "aws_s3_bucket" "iceberg" {
  bucket        = "${var.environment}-ray-iceberg-${data.aws_caller_identity.current.account_id}"
  force_destroy = false
}

resource "aws_glue_catalog_database" "iceberg" {
  name = "ray_iceberg_db_${var.environment}"
}

# -------------------------------------------------------------
# 3. EKS CLUSTER
# -------------------------------------------------------------
resource "aws_iam_role" "cluster" {
  name = "${var.environment}-eks-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cluster_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.cluster.name
}

resource "aws_eks_cluster" "main" {
  name     = "${var.environment}-ray-eks"
  role_arn = aws_iam_role.cluster.arn
  version  = "1.30"

  vpc_config {
    subnet_ids              = [aws_subnet.private_1.id, aws_subnet.private_2.id]
    endpoint_private_access = true
    endpoint_public_access  = true
  }

  depends_on = [aws_iam_role_policy_attachment.cluster_policy]
}

# -------------------------------------------------------------
# 4. EKS NODE GROUPS (System, Spot Workers, Specialized Actor Pool)
# -------------------------------------------------------------
resource "aws_iam_role" "node" {
  name = "${var.environment}-eks-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node_worker" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.node.name
}

resource "aws_iam_role_policy_attachment" "node_cni" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.node.name
}

resource "aws_iam_role_policy_attachment" "node_ecr" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.node.name
}

# Node Pool 1: System (On-Demand for Head pod & Core services)
resource "aws_eks_node_group" "system" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "system-pool"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = [aws_subnet.private_1.id, aws_subnet.private_2.id]

  capacity_type  = "ON_DEMAND"
  instance_types = ["m6i.xlarge"]

  scaling_config {
    desired_size = 2
    min_size     = 1
    max_size     = 3
  }

  labels = {
    node-role = "system"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr
  ]
}

# Node Pool 2: Spot Workers for Cost Reduction
resource "aws_eks_node_group" "spot_workers" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "ray-spot-workers"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = [aws_subnet.private_1.id, aws_subnet.private_2.id]

  capacity_type  = "SPOT"
  instance_types = ["m6i.xlarge", "m5.xlarge", "c6i.xlarge", "c5.xlarge"]

  scaling_config {
    desired_size = 2
    min_size     = 0
    max_size     = 10
  }

  labels = {
    node-role = "ray-worker"
  }

  taint {
    key    = "ray-worker"
    value  = "true"
    effect = "PREFER_NO_SCHEDULE"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr
  ]
}

# Node Pool 3: Specialized Workers for EntityResolver Actor
resource "aws_eks_node_group" "actor_workers" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "ray-actor-workers"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = [aws_subnet.private_1.id, aws_subnet.private_2.id]

  capacity_type  = "ON_DEMAND"
  instance_types = ["m6i.xlarge"]

  scaling_config {
    desired_size = 1
    min_size     = 1
    max_size     = 3
  }

  labels = {
    node-role = "specialized-actor"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr
  ]
}

# -------------------------------------------------------------
# 5. IRSA (Zero-Trust IAM for Pods)
# -------------------------------------------------------------
data "tls_certificate" "eks" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

data "aws_iam_policy_document" "irsa_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub"
      values   = ["system:serviceaccount:default:ray-sa"]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ray_irsa" {
  name               = "${var.environment}-ray-pod-irsa-role"
  assume_role_policy = data.aws_iam_policy_document.irsa_trust.json
}

resource "aws_iam_policy" "ray_s3_glue" {
  name = "${var.environment}-ray-s3-glue-access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:*"]
        Resource = [aws_s3_bucket.iceberg.arn, "${aws_s3_bucket.iceberg.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ray_irsa_attach" {
  role       = aws_iam_role.ray_irsa.name
  policy_arn = aws_iam_policy.ray_s3_glue.arn
}
