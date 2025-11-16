terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = "trusty-anchor-473006-u9"
  region  = "europe-west9"
  zone    = "europe-west9-a"
}

resource "google_compute_instance" "vm-example-0" {
  name         = "vm-example-0"
  machine_type = "e2-medium"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
    }
  }

  network_interface {
    network = "default"

    access_config {
      # pour avoir une IP publique
    }
  }
}

resource "google_compute_instance" "vm-example-1" {
  name         = "vm-example-1"
  machine_type = "e2-medium"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
    }
  }

  network_interface {
    network = "default"

    access_config {
      # pour avoir une IP publique
    }
  }
}
