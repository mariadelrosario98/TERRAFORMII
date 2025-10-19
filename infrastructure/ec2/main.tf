resource "aws_instance" "this" {
  ami                         = data.aws_ami.this.id
  instance_type               = var.instance_type
  key_name                    = aws_key_pair.this.key_name 
  vpc_security_group_ids      = [aws_security_group.this.id] 
  user_data                   = var.user_data
  iam_instance_profile = aws_iam_instance_profile.ssm_instance_profile.name
  depends_on = [
    aws_vpc_security_group_ingress_rule.this,
  ]
}
# Add a policy that grants S3 permissions to the SSM role
resource "aws_iam_role_policy" "s3_writer_policy" {
  name = "s3-writer-policy"
  role = aws_iam_role.ssm_role.name # Attach to the existing SSM role

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        # Grant permissions to create and put objects in the specific bucket
        Action = [
          "s3:CreateBucket",
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket", # Useful for checking the bucket
        ],
        Resource = [
          "arn:aws:s3:::terraform-51257688b24ec567",
          "arn:aws:s3:::terraform-51257688b24ec567/*",
        ],
      },
    ],
  })
}

resource "aws_iam_role" "ssm_role" {
  name = "EC2-SSM-Access-Role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
      },
    ],
  })
}

resource "null_resource" "reboot_instance_on_change" {
  # 1. Trigger: Hash the user_data to detect changes
  triggers = {
    user_data_checksum = md5(aws_instance.this.user_data)
  }

  # Ensure the instance is up and ready before attempting to connect
  depends_on = [aws_instance.this]

  # 2. Action: Execute reboot via SSH
  provisioner "remote-exec" {
    # Command to run on the EC2 instance
    inline = [
      "echo 'User data change detected. Initiating reboot...'",
      "sudo reboot",
    ]

    connection {
      type        = "ssh"
      user        = "ubuntu" # Standard user for the Ubuntu 22.04 AMI
      host        = aws_instance.this.public_ip
      
      # !!! CRITICAL: Replace the path below with the path to your PRIVATE key !!!
      private_key = file("C:/Users/ASUS/Documents/Maestria Ciencia de los Datos/TERCER SEMESTRE/MINERIA DE GRANDES VOLUMENES INFO/TALLER 4/Archive (4)/infrastructure/ec2/.shh/key") 
      
      timeout     = "45s" # Allows the reboot command to start before the connection drops
      agent       = false 
    }
  }
}

# 2. Attach the necessary AWS Managed Policy
resource "aws_iam_role_policy_attachment" "ssm_policy_attach" {
  role       = aws_iam_role.ssm_role.name
  # This policy is the minimum required for Session Manager
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# 3. Create the Instance Profile to attach to the EC2 instance
resource "aws_iam_instance_profile" "ssm_instance_profile" {
  name = "EC2-SSM-Instance-Profile1"
  role = aws_iam_role.ssm_role.name
}

data "aws_ami" "this" {
  most_recent = true

  owners = ["099720109477"] # Canonical

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_key_pair" "this" {
  key_name   = "my-ec2-key"
  public_key = file("C:/Users/ASUS/Documents/Maestria Ciencia de los Datos/TERCER SEMESTRE/MINERIA DE GRANDES VOLUMENES INFO/TALLER 4/Archive (4)/infrastructure/ec2/.shh/key.pub")
}

resource "aws_security_group" "this" {
  name        = "security_group"
  description = "Allow TLS inbound traffic and all outbound traffic"
}

resource "aws_vpc_security_group_ingress_rule" "this" {
  security_group_id = aws_security_group.this.id
  cidr_ipv4         = "0.0.0.0/0" # use more restrictions in a production setting
  from_port         = 22
  ip_protocol       = "tcp"
  to_port           = 22
}


resource "aws_vpc_security_group_egress_rule" "this" {
  security_group_id = aws_security_group.this.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1" # semantically equivalent to all ports
}
