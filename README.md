# OpenRelik Worker - KStrike & UAL Timeliner

A worker for [OpenRelik](https://openrelik.org/) that provides two tasks for parsing Windows Server User Access Logging (UAL) from Server 2012 and newer systems:

- **KStrike UAL Parser** — Parses individual `.mdb` files using [KStrike](https://github.com/brimorlabs/KStrike) by Brian Moran. Features IP-to-hostname correlation when the DNS table is present.
- **UAL Timeliner** — Builds forensic timelines from one or more `.mdb` files using [UAL Timeliner](https://github.com/kev365/ual-timeliner) by Kevin Stokes. Extracts events from CLIENTS, DNS, and ROLE_ACCESS tables into a sorted, deduplicated timeline with multiple output format support.

## KStrike UAL Parser

Parses `.mdb` files individually into double-pipe `||` delimited text output.

### Features

- Auto-filters `.mdb` files from input
- Disk image support — searches known UAL paths inside mounted images
- Combine & deduplicate — merge all parsed output into a single file with duplicate rows removed
- Row-based output splitting — configurable max rows per file (default 500,000) when combining
- Combine-only mode — accepts previously parsed KStrike `.txt` files to combine without re-parsing
- Output prefix — optionally prefix output filenames

### Configuration

| Option | Type | Description |
|--------|------|-------------|
| File prefix | Text | Optional prefix for output filenames |
| Combine & dedup | Checkbox | Merge all output into one deduplicated file |
| Max rows per file | Text | Row limit per output file when combining (default: 500000, 0 = no limit) |

## UAL Timeliner

Groups all input `.mdb` files together and builds a unified chronological timeline. Extracts `InsertDate`, `LastAccess`, `FirstSeen`, `LastSeen`, and optionally `Day###` historical access data. Automatically skips `SystemIdentity.mdb` and handles ESE databases in dirty shutdown state.

### Capabilities

- Groups all `.mdb` files into a single timeline run
- Disk image support — searches known UAL paths inside mounted images
- Multiple output formats — CSV, XLSX, SQLite, Parquet, and K2T (Timesketch JSONL)
- Multi-format output — select multiple formats in a single run
- Deduplication — removes duplicate entries preferring `Current.mdb` over archive databases
- Full output mode — includes extra columns and Day### daily access history
- Row-based file splitting — for CSV, K2T, and XLSX formats (default 500,000 rows)
- Role GUID resolution — maps 21 known Windows Server role GUIDs to human-readable names

### Options

| Option | Type | Description |
|--------|------|-------------|
| File prefix | Text | Optional prefix for output filenames |
| Output format | Multi-select | One or more formats: csv, xlsx, sqlite, parquet, k2t (default: csv) |
| Full output | Checkbox | Include all columns and parse Day### historical data |
| Deduplication | Select | Enable/disable dedup (default: true) |
| Max rows per file | Text | Row limit per output file for CSV, K2T, XLSX (default: 500000, 0 = no limit) |

## Project Structure

```text
src/
  app.py              # Celery app initialization
  tasks.py            # Worker task definitions (KStrike + UAL Timeliner)
  kstrike.py          # KStrike UAL parser (from brimorlabs/KStrike)
  ual_timeliner.py    # UAL Timeliner (from kev365/ual-timeliner)
tests/
  test_tasks.py       # Unit tests (27 tests)
  Sample_UAL/         # Sample MDB file for testing
```

## Licenses

- **This worker**: Apache License 2.0 ([LICENSE](LICENSE))
- **KStrike**: BSD-like with acknowledgment clause / GPL v3 dual license ([LICENSE-KSTRIKE](LICENSE-KSTRIKE))
- **UAL Timeliner**: MIT License ([LICENSE-UAL-TIMELINER](LICENSE-UAL-TIMELINER))

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
