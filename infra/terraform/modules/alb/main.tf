# The ONLY public-ingress point in the entire Terraform design. The
# `alb` security group (0.0.0.0/0 on 80/443 only) is created by the
# `networking` module (Task 6) and passed in here as
# `var.alb_security_group_id` -- this module attaches the ALB to that
# existing security group rather than creating any new 0.0.0.0/0 rule of
# its own. No other module in this tree should define a resource
# reachable from 0.0.0.0/0; every internal service (observability,
# messaging, app services other than go-server's target group) sits
# behind the `internal`/`app` security groups instead.

resource "aws_lb" "main" {
  name_prefix        = "cr${substr(var.environment, 0, 3)}"
  load_balancer_type = "application"
  security_groups    = [var.alb_security_group_id]
  subnets            = var.public_subnet_ids
}

# Health check uses GET /metrics rather than a dedicated /healthz --
# design doc's explicit, disclosed pragmatic choice: no dedicated health
# endpoint exists yet on the Go server, and /metrics is already exposed
# and cheap to scrape for this purpose.
resource "aws_lb_target_group" "go_server" {
  name_prefix = "crgs-"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/metrics"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 15
    timeout             = 5
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"

    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.main.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.go_server.arn
  }
}
