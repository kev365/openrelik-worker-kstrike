<!--
README for the OpenRelik Worker Template

This file provides instructions on how to use this template to create a new OpenRelik worker.
The placeholder `kstrike` needs to be replaced with the actual name of your worker.
The `bootstrap.sh` script is designed to help with this process.
-->

3.  **Write Tests:**
    Before or alongside developing your worker's core logic, start creating tests.
    *   **Unit Tests:** Create unit tests for individual functions and classes within your worker's `src` directory. Place these in the `tests/` directory.
    *   Refer to the "Test" section below for instructions on how to run your tests.
4.  **Implement Worker Logic:**
    Fill in the `src/tasks.py` file (and any other necessary modules) with the core functionality of your worker.


# Openrelik worker kstrike
## Description
KStrike is a parser for User Access Logging from Server 2012 and newer systems. Created by Brian Moran, this tool features corralation of IP addresses to hostnames when the DNS table is present.

## Deploy
Add the below configuration to the OpenRelik docker-compose.yml file.

```
openrelik-worker-kstrike:
    container_name: openrelik-worker-kstrike
    image: ghcr.io/kev365/openrelik-worker-kstrike:latest
    restart: always
    environment:
      - REDIS_URL=redis://openrelik-redis:6379
      - OPENRELIK_PYDEBUG=0
    volumes:
      - ./data:/usr/share/openrelik/data
    command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-kstrike"
    # ports:
      # - 5678:5678 # For debugging purposes.
```


## Deploy via Helm
Add the below configuration to your custom values.yml deployment file.

```
openrelik:
  workers:
    # Add this section below, under openrelik.workers
    - name: openrelik-worker-kstrike
      image: ghcr.io/kev365/openrelik-worker-kstrike:latest
      command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-kstrike"
      env: {}
      resources: {}
```

## Deploy via Terraform
```
resource "kubernetes_deployment" "openrelik_worker_kstrike" {
  metadata {
    name = "openrelik-worker-kstrike"
  }

  spec {
    replicas = 1

    selector {
      match_labels = { app = "openrelik-worker-kstrike" }
    }

    template {
      metadata {
        labels = { app = "openrelik-worker-kstrike" }
      }

      spec {
        container {
          name  = "openrelik-worker-kstrike"
          image = "ghcr.io/kev365/openrelik-worker-kstrike:latest"
          command = [
            "celery", "--app=src.app", "worker", "--task-events", "--concurrency=4", "--loglevel=INFO", "-Q", "openrelik-worker-kstrike",
          ]

          env {
            name  = "REDIS_URL"
            value = "redis://openrelik-redis:6379"
          }
          env {
            name  = "OPENRELIK_PYDEBUG"
            value = "0"
          }

          volume_mount {
            name       = "data-volume"
            mount_path = "/usr/share/openrelik/data"
          }
        }

        volume {
          name = "data-volume"
          persistent_volume_claim {
            claim_name = "data-pvc"
          }
        }
      }
    }
  }
}
```

## Test
```
uv sync --group test
uv run pytest -s --cov=.
```

OR

```
pip install pytest-cov
pip install openrelik-common
pytest -s --cov=.
```
