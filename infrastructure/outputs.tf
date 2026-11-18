output "ec2_public_ip" {
  value       = aws_instance.app_server.public_ip
  description = "Public IP address of the EC2 instance (use this for your EC2_HOST secret)"
}