data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.environment}-chainroute-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.environment}-chainroute-igw" }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.environment}-chainroute-public-${count.index}" }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index + 2)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags              = { Name = "${var.environment}-chainroute-private-${count.index}" }
}

# Single NAT gateway (in the first public subnet) for private-subnet
# egress -- documented cost/availability tradeoff (design doc §4.2): a
# single NAT is a single point of failure across AZs for outbound
# traffic; a per-AZ NAT gateway would double this specific cost for
# redundancy this portfolio-scale deployment doesn't need.
resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "${var.environment}-chainroute-nat-eip" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags          = { Name = "${var.environment}-chainroute-nat" }
  depends_on    = [aws_internet_gateway.main]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "${var.environment}-chainroute-public-rt" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }
  tags = { Name = "${var.environment}-chainroute-private-rt" }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# Security groups -- only the ALB accepts public ingress anywhere in this
# design (design doc §4.2). Every other SG's ingress is scoped to another
# SG, never a CIDR block.
resource "aws_security_group" "alb" {
  name_prefix = "${var.environment}-chainroute-alb-"
  vpc_id      = aws_vpc.main.id
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.environment}-chainroute-alb-sg" }
}

resource "aws_security_group" "app" {
  name_prefix = "${var.environment}-chainroute-app-"
  vpc_id      = aws_vpc.main.id
  ingress {
    description     = "go-server HTTP from ALB only"
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.environment}-chainroute-app-sg" }
}

resource "aws_security_group" "internal" {
  name_prefix = "${var.environment}-chainroute-internal-"
  vpc_id      = aws_vpc.main.id
  description = "Shared SG for cpp-router, redpanda, observability, and any service-to-service traffic -- ingress rules are added per-consumer by later modules via aws_security_group_rule, not baked in here, to avoid a monolithic SG with rules for every port up front."
  ingress {
    description = "Self-referencing: allows all TCP traffic between services that share this security group (e.g. go-worker -> Redpanda, future observability scraping) -- proportionate for a single shared internal-tier SG rather than enumerating every port pair"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }
  ingress {
    description     = "cpp-router gRPC (50051) from the app-tier SG (go-server)"
    from_port       = 50051
    to_port         = 50051
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.environment}-chainroute-internal-sg" }
}

resource "aws_security_group" "data" {
  name_prefix = "${var.environment}-chainroute-data-"
  vpc_id      = aws_vpc.main.id
  ingress {
    description     = "Postgres from app/worker services only"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id, aws_security_group.internal.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.environment}-chainroute-data-sg" }
}

resource "aws_service_discovery_private_dns_namespace" "internal" {
  name = "chainroute.local"
  vpc  = aws_vpc.main.id
}
