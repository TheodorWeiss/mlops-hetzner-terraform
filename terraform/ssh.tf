resource "hcloud_ssh_key" "deploy_key" {
  name       = "${var.server_name}-deploy-key"
  public_key = file(var.ssh_public_key_path)
}
