output "server_ip" {
  description = "Public IPv4 address of the new MLOps server"
  value       = hcloud_server.mlops.ipv4_address
}

output "server_name" {
  description = "Name of the new Terraform-managed server"
  value       = hcloud_server.mlops.name
}

output "volume_id" {
  description = "ID of the attached Hetzner volume"
  value       = hcloud_volume.data.id
}

output "volume_name" {
  description = "Name of the attached Hetzner volume"
  value       = hcloud_volume.data.name
}
