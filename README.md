# OpenRelik Worker - KStrike

A worker for [OpenRelik](https://openrelik.org/) that parses User Access Logging (UAL) from Windows Server 2012 and newer systems using [KStrike](https://github.com/brimorlabs/KStrike) by Brian Moran. Features correlation of IP addresses to hostnames when the DNS table is present.

## Features

- **Auto-filters `.mdb` files** from input — no manual file selection needed
- **Disk image support** — automatically searches known UAL paths inside mounted images
- **Combine & deduplicate** — merge all parsed output into a single file with duplicate rows removed
- **Row-based output splitting** — configurable max rows per file (default 500,000) when combining
- **Combine-only mode** — accepts previously parsed KStrike `.txt` files to combine without re-parsing
- **Output prefix** — optionally prefix output filenames for organization

## Worker Configuration

| Option | Type | Description |
|--------|------|-------------|
| File prefix | Text | Optional prefix for output filenames (e.g., `mycase` produces `mycase_filename.txt`) |
| Combine & dedup | Checkbox | Merge all output into one deduplicated file. Also enables combine-only mode for `.txt` input. |
| Max rows per file | Text | Row limit per output file when combining (default: 500000, 0 = no limit) |

Output files are double-pipe `||` delimited, UTF-8 encoded text.

## Project Structure

```text
src/
  app.py              # Celery app initialization
  tasks.py            # Worker task definition
  kstrike.py          # KStrike UAL parser (from brimorlabs/KStrike, wrapped for import)
tests/
  test_tasks.py       # Unit tests
  Sample_UAL/         # Sample MDB file for testing
```

## Run Tests

```bash
uv sync --group test
uv run pytest tests/test_tasks.py -v
```

## Deploy

Add the below configuration to the OpenRelik docker-compose.yml file.

```yaml
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
```

## Deploy via Helm

Add the below configuration to your custom values.yml deployment file.

```yaml
openrelik:
  workers:
    - name: openrelik-worker-kstrike
      image: ghcr.io/kev365/openrelik-worker-kstrike:latest
      command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-kstrike"
      env: {}
      resources: {}
```

## Deploy via Terraform

```hcl
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
