variable "aws_region" {
  default = "eu-north-1"
}

variable "instance_type" {
  default = "m7i.xlarge" # Recommended for running docker-compose pipelines
}

variable "key_name" {
  description = "Name of an existing EC2 Key Pair in AWS to enable SSH access"
  type        = string
  default = "breta"
}