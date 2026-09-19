output "alb_dns_name" {
  value = module.alb.dns_name
}

output "ecr_go_server_repository_url" {
  value = aws_ecr_repository.go_server.repository_url
}

output "ecr_go_worker_repository_url" {
  value = aws_ecr_repository.go_worker.repository_url
}

output "ecr_cpp_router_repository_url" {
  value = aws_ecr_repository.cpp_router.repository_url
}

output "database_secret_arn" {
  value = module.database.secret_arn
}

output "cloudfront_domain_name" {
  # one() avoids indexing a count-0 module in the disabled path (see the
  # matching CHAINROUTE_CORS_ALLOWED_ORIGINS comment in main.tf) -- it
  # returns null when enable_frontend = false, the single domain name
  # otherwise.
  value = one(module.frontend[*].cloudfront_domain_name)
}
