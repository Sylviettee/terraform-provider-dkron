terraform {
  required_providers {
    dkron = {
      version = "0.3"
      source  = "registry.terraform.io/sylviettee/dkron"
    }
  }
}

provider "dkron" {
  host = "http://localhost:8080/v1"
}

resource "dkron_job" "job1_dms" {
  name        = "job1_dms_1"
  timezone    = "Europe/Riga"
  displayname = "job dms 1"
  schedule    = "@every 10m"
  owner       = "Gitlab"
  owner_email = "gitlab@gitlabovich.com"
  disabled    = false
  retries     = 5
  concurrency = "forbid"
  executor    = "shell"

  executor_config = {
    "shell"   = true
    "command" = "ls"
    "env"     = "FOO=bar"
  }

  tags = {
    "dms" = "cron:1"
  }

  # output to stdin/stdout
  processors {
    type    = "log"
  }
}
