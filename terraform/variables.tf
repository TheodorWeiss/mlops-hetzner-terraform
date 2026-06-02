variable "hcloud_token" {
  description = "Hetzner Cloud API token"
  type        = string
  sensitive   = true
}

variable "ssh_public_key_path" {
  description = "Path to the public SSH key on the local machine"
  type        = string
}

variable "allowed_ssh_ip" {
  description = "Public IP address allowed to connect via SSH, in CIDR format, for example 1.2.3.4/32"
  type        = string
}

variable "location" {
  description = "Hetzner location for server and volume"
  type        = string
  default     = "nbg1"

  validation {
    condition     = contains(["nbg1", "fsn1"], var.location)
    error_message = "Location must be either nbg1 or fsn1."
  }
}

variable "server_name" {
  description = "Name of the new Terraform-managed MLOps server"
  type        = string
  default     = "mlops-exam-cx32"
}
