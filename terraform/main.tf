provider "hcloud" {
  token = var.hcloud_token
}

resource "hcloud_server" "mlops" {
  name        = var.server_name
  image       = "ubuntu-24.04"
  server_type = "cx33"
  location    = var.location

  ssh_keys     = [hcloud_ssh_key.deploy_key.id]
  firewall_ids = [hcloud_firewall.mlops_firewall.id]

  user_data = templatefile("${path.module}/cloud-init.yaml", {
    ssh_public_key = file(var.ssh_public_key_path)
    volume_id      = hcloud_volume.data.id
  })

  labels = {
    project = "mlops-exam"
    managed = "terraform"
  }
}

resource "hcloud_volume" "data" {
  name     = "${var.server_name}-data"
  size     = 50
  location = var.location

  labels = {
    project = "mlops-exam"
    managed = "terraform"
    purpose = "mlops-data"
  }
}

resource "hcloud_volume_attachment" "data_attachment" {
  volume_id = hcloud_volume.data.id
  server_id = hcloud_server.mlops.id
  automount = false
}
