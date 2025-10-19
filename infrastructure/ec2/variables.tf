variable "region" {
  default = "us-east-1"
}

# variables.tf
variable "instance_type" {
  description = "The type of the EC2 instance (e.g., m5.2xlarge)."
  type        = string
  default     = "m5.2xlarge"
}

# Add a separate variable for user_data to keep it clean (Optional, but good practice)
variable "instance_user_data" {
  description = "The cloud-init script to run on instance bootstrap."
  type        = string
  default     = <<-EOF
      #!/bin/bash
      apt-get update -y
      apt-get install -y python3 python3-pip -y # Use -y for non-interactive install
      pip3 install pandas
      EOF
}

variable "public_ssh_key" {
  description = "Public key to be able to connect to the instance using ssh"
}

variable "ssh_allowed_cidrs" {
  type    = list(string)
  default = ["0.0.0.0/0"] # You should restrict this for production
}

variable "user_data" {
    description = "Startup script to execute when creating an instance"
}