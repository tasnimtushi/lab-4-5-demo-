# Lab 4.5 — Pipeline Dockerization: Comprehensive Flow Problem Report

This document provides a detailed technical audit of the **Lab 4.5** flow described in `R&D.md`. Every issue is catalogued by severity, root cause, exact impact, and recommended remediation.

---

## Executive Summary

While the conceptual architecture of Lab 4.5 (Redpanda + Faust Stream Processor + Docker Compose + Healthchecks) is sound, the implementation details in `R&D.md` contain **fatal execution-halting bugs** as well as multiple operational flaws. Most notably:
1. **Fatal Runtime Crash:** `app.web.router.add_get` does not exist on Faust's `Web` service object. Importing `src.streaming` immediately crashes with `AttributeError`.
2. **Dependency Incompatibility:** `aiohttp==3.11.18` contains breaking routing internals that break `faust-streaming 0.11.3` and its dependency `aiohttp-cors`.
3. **Module Resolution Failure:** The Dockerfile omits `ENV PYTHONPATH=/app`, risking `ModuleNotFoundError: No module named 'src'` when executing as a non-root user.
4. **Healthcheck Misleading Semantics:** The `/health` endpoint unconditionally returns HTTP 200 without probing broker connectivity, masking broken streaming states.
5. **Windows Execution Failures:** Line endings (CRLF) and Linux-only shell idioms (`chmod +x`) break execution on Windows host environments.

---

## 🔴 Critical Severity (Will Completely Break the Lab Flow)

### Issue P-01: `app.web.router.add_get` Is Invalid Faust API
- **Location:** `R&D.md` — Chapter 3 (`src/streaming.py`, lines 56–63)
- **Problematic Code:**
  ```python
  async def healthcheck(request):
      return web.json_response({
          "status": "healthy",
          "service": "datahive-pipeline",
      })

  app.web.router.add_get("/health", healthcheck)
  ```
- **Root Cause:**
  In Faust, `app.web` is an instance of Faust's internal `faust.web.base.Web` service driver, **not** an `aiohttp.web.Application`. Faust constructs the underlying web server lazily. The `Web` class does not possess a `.router` attribute.
- **Impact:**
  When `faust -A src.streaming worker` imports the module at container startup, Python raises:
  ```text
  AttributeError: 'Web' object has no attribute 'router'
  ```
  The container crashes immediately, exits with code 1, and enters a restart loop. The healthcheck endpoint is never served, causing Docker Compose to declare the service `unhealthy`.
- **Remediation:**
  Use Faust's native page decorator `@app.page("/path")` or subclass `faust.web.View`:
  ```python
  @app.page("/health")
  async def healthcheck(self, request):
      return self.json({
          "status": "healthy",
          "service": "datahive-pipeline",
      })
  ```

---

### Issue P-02: `aiohttp==3.11.18` Architectural Incompatibility
- **Location:** `R&D.md` — Chapter 2 (`requirements.txt`)
- **Problematic Code:**
  ```text
  faust-streaming==0.11.3
  aiohttp==3.11.18
  ```
- **Root Cause:**
  `aiohttp` 3.11 introduced major internal restructuring of `web.UrlDispatcher` and routing resolution. `faust-streaming==0.11.3` depends on `aiohttp-cors` (v0.7.0/0.8.1), which relies on deprecated internal routing structures from `aiohttp < 3.10`.
- **Impact:**
  Pairing `aiohttp 3.11.x` with `faust-streaming 0.11.3` triggers runtime routing errors or CORS registration failures when the web driver initializes.
- **Remediation:**
  Pin a tested, compatible release of `aiohttp` (such as `3.9.5` or `~=3.9.0`):
  ```text
  faust-streaming==0.11.3
  aiohttp==3.9.5
  ```

---

### Issue P-03: Missing `PYTHONPATH=/app` in Dockerfile
- **Location:** `R&D.md` — Chapter 4 (`Dockerfile`)
- **Problematic Code:**
  ```dockerfile
  WORKDIR /app
  ...
  USER appuser
  CMD ["faust", "-A", "src.streaming", "worker", "-l", "info"]
  ```
- **Root Cause:**
  When executing Python binaries or entrypoint scripts under a non-root user without an explicit package installation (`pip install -e .`), `sys.path` does not reliably include the current working directory in all execution contexts.
- **Impact:**
  Container startup fails intermittently with:
  ```text
  ModuleNotFoundError: No module named 'src'
  ```
- **Remediation:**
  Explicitly set the environment variable in the `Dockerfile`:
  ```dockerfile
  WORKDIR /app
  ENV PYTHONPATH=/app
  ```

---

## 🟠 High Severity (Silent Failures & Deceptive Operational State)

### Issue P-04: Redpanda Healthcheck Non-Standard Exit Codes
- **Location:** `R&D.md` — Chapter 5 (`compose.yaml`)
- **Problematic Code:**
  ```yaml
  healthcheck:
    test: ["CMD", "rpk", "cluster", "health"]
    interval: 10s
    timeout: 5s
    retries: 10
    start_period: 20s
  ```
- **Root Cause:**
  When Redpanda is initializing or in degraded consensus, `rpk cluster health` exits with exit code **10** (not standard 0 or 1). Some versions of Docker Engine treat unexpected non-zero exit codes as hard probe errors, triggering premature unhealthiness before `start_period` elapses.
- **Impact:**
  Redpanda can be marked `unhealthy` during leader election, causing `depends_on: { condition: service_healthy }` to block the `pipeline` container indefinitely.
- **Remediation:**
  Use shell pattern matching against stdout:
  ```yaml
  healthcheck:
    test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
  ```

---

### Issue P-05: Unconditional `/health` Check Lacks Broker Connectivity Verification
- **Location:** `R&D.md` — Chapter 3 (`src/streaming.py`) & Chapter 8
- **Root Cause:**
  The healthcheck handler simply returns `{"status": "healthy"}` unconditionally. It never verifies whether Faust has active connections to Kafka/Redpanda or whether consumer partitions are assigned.
- **Impact:**
  If the broker fails, the network disconnects, or credentials fail, the container still reports `(healthy)` in `docker compose ps`. This creates a dangerous false positive where monitoring systems assume the pipeline is ingesting data when it is completely dead.
- **Remediation:**
  Validate consumer transport status before returning HTTP 200:
  ```python
  @app.page("/health")
  async def healthcheck(self, request):
      transport_connected = (
          app.consumer is not None
          and not getattr(app.consumer, "_closed", True)
      )
      if transport_connected:
          return self.json({"status": "healthy", "service": "datahive-pipeline"}, status=200)
      return self.json({"status": "starting", "service": "datahive-pipeline"}, status=503)
  ```

---

### Issue P-06: Aggressive Healthcheck Timeouts on Cold Start
- **Location:** `R&D.md` — Chapter 5 (`compose.yaml`)
- **Problematic Code:**
  ```yaml
  start_period: 40s
  interval: 15s
  retries: 3
  ```
- **Root Cause:**
  On cold starts (or systems with limited CPU/IOPS), Redpanda Raft initialization followed by Faust group join and aiohttp initialization often exceeds 40–50 seconds.
- **Impact:**
  With only 3 retries at 15s intervals, the pipeline container is marked `unhealthy` after ~45s, triggering restarts before the application has completed its legitimate startup.
- **Remediation:**
  Increase `start_period` to `60s` and `retries` to `5`.

---

## 🟡 Medium Severity (Host Portability & Resource Safety)

### Issue P-07: Missing Explicit Bridge Network Definition
- **Location:** `R&D.md` — Chapter 5 (`compose.yaml`)
- **Root Cause:**
  Services rely on the auto-generated default network.
- **Impact:**
  When expanding the infrastructure or incorporating additional compose files (`-f`), default networks can suffer from service discovery collisions or unpredictable DNS alias resolution.
- **Remediation:**
  Declare a top-level `networks:` block with a dedicated network (e.g., `datahive`).

---

### Issue P-08: Missing `value_serializer='raw'` on Faust Topic
- **Location:** `R&D.md` — Chapter 3 (`src/streaming.py`)
- **Problematic Code:**
  ```python
  events_topic = app.topic("datahive-events", value_type=bytes)
  ```
- **Root Cause:**
  Faust defaults to its internal serialization mechanisms unless explicitly set to raw byte mode. When external producers (such as `rpk topic produce`) stream plaintext or JSON, Faust can fail during payload framing.
- **Remediation:**
  Add `value_serializer="raw"` to `app.topic(...)`.

---

### Issue P-09: Windows Incompatibility with `chmod +x`
- **Location:** `R&D.md` — Chapter 9
- **Problematic Instructions:**
  ```bash
  chmod +x scripts/publish_test_event.sh
  ./scripts/publish_test_event.sh
  ```
- **Root Cause:**
  Windows NTFS filesystems do not support POSIX execution bits, and PowerShell/CMD do not recognize `chmod`.
- **Impact:**
  Users running on Windows PowerShell receive `CommandNotFoundException` and cannot execute the shell script directly.
- **Remediation:**
  Document execution via `bash scripts/publish_test_event.sh` or provide a PowerShell alternative.

---

### Issue P-10: Windows CRLF Line Endings Break Container Execution
- **Location:** `R&D.md` — Chapter 6 / Chapter 9
- **Root Cause:**
  When Git or VS Code creates `publish_test_event.sh` on Windows, it often uses CRLF (`\r\n`). When fed to a Linux container, the shebang evaluates to `#!/usr/bin/env bash\r`.
- **Impact:**
  Throws `/usr/bin/env: 'bash\r': No such file or directory`.
- **Remediation:**
  Include `.gitattributes` with `*.sh text eol=lf`.

---

### Issue P-11: Absence of Container Resource Limits
- **Location:** `R&D.md` — Chapter 5 (`compose.yaml`)
- **Root Cause:**
  Neither service sets CPU or memory bounds in Docker Compose.
- **Impact:**
  Redpanda and Faust can consume unbounded memory under spike loads, leading to host starvation or non-deterministic OS OOM kills.
- **Remediation:**
  Configure `deploy.resources.limits.memory` (e.g., 1.5G for Redpanda, 512M for pipeline).

---

### Issue P-12: Missing `restart` Policy on Redpanda Broker
- **Location:** `R&D.md` — Chapter 5 (`compose.yaml`)
- **Root Cause:**
  `pipeline` has `restart: unless-stopped`, but `redpanda` has no restart configuration.
- **Impact:**
  If Redpanda crashes or runs out of memory, Docker will not restart it, causing the streaming pipeline to become permanently disconnected.
- **Remediation:**
  Add `restart: unless-stopped` to the `redpanda` service.

---

## 🔵 Low Severity (Documentation & Observability Nuances)

### Issue P-13: Docker Compose Health Status Visibility
- **Location:** `R&D.md` — Chapter 7 & 8
- **Observation:** In Docker Compose v2 versions prior to 2.20, `docker compose ps` does not show health tags in the default table output. Providing `docker inspect` ensures learners can verify container health across any Docker CLI version.

### Issue P-14: Role of `src/__init__.py` Left Unexplained
- **Location:** `R&D.md` — Chapter 3
- **Observation:** The project structure includes `src/__init__.py` without explaining its necessity. Learners who skip or omit empty files will face `ModuleNotFoundError`.

### Issue P-15: Premature Log Inspection Timing
- **Location:** `R&D.md` — Chapter 7
- **Observation:** Running `docker compose logs` immediately after `docker compose up -d` displays broker connection-retry noise while Raft is electing leader partitions. Learners mistake this normal initial handshake for failure. The guide should instruct waiting for `(healthy)` status.

### Issue P-16: Ephemeral Storage Across Stack Teardown
- **Location:** `R&D.md` — Chapter 10
- **Observation:** Neither Redpanda nor Faust mounts persistent host volumes. Running `docker compose down` destroys all topics and consumer group offsets. The lab should clearly highlight that broker data is disposable.

---

## Defect Matrix & Verification Summary

| ID | Component | Severity | Description | Status in Corrected Codebase |
|:---|:---|:---:|:---|:---:|
| **P-01** | `src/streaming.py` | 🔴 Critical | `app.web.router.add_get` causes `AttributeError` | **Fixed** (uses `@app.page`) |
| **P-02** | `requirements.txt` | 🔴 Critical | `aiohttp==3.11.18` breaks `aiohttp-cors` | **Fixed** (pinned to `3.9.5`) |
| **P-03** | `Dockerfile` | 🔴 Critical | Missing `PYTHONPATH=/app` causes import error | **Fixed** (`ENV PYTHONPATH=/app`) |
| **P-04** | `compose.yaml` | 🟠 High | Redpanda healthcheck exit code 10 | **Fixed** (`CMD-SHELL` with `grep`) |
| **P-05** | `src/streaming.py` | 🟠 High | Healthcheck does not check broker transport | **Fixed** (checks `app.consumer`) |
| **P-06** | `compose.yaml` | 🟠 High | Cold start healthcheck too aggressive | **Fixed** (`start_period: 60s`, `retries: 5`) |
| **P-07** | `compose.yaml` | 🟡 Medium | No explicit named network | **Fixed** (`networks: [datahive]`) |
| **P-08** | `src/streaming.py` | 🟡 Medium | Missing `value_serializer="raw"` on topic | **Fixed** (`value_serializer="raw"`) |
| **P-09** | `scripts/` | 🟡 Medium | `chmod +x` is non-functional on Windows | **Fixed** (documented `bash` invocation) |
| **P-10** | `.gitattributes` | 🟡 Medium | CRLF line endings break shell shebang | **Fixed** (`*.sh text eol=lf`) |
| **P-11** | `compose.yaml` | 🟡 Medium | No resource limits on containers | **Fixed** (`deploy.resources.limits`) |
| **P-12** | `compose.yaml` | 🟡 Medium | Missing `restart` policy on Redpanda | **Fixed** (`restart: unless-stopped`) |
| **P-13** | Docs | 🔵 Low | CLI health status visibility differences | **Fixed** (added `docker inspect`) |
| **P-14** | Docs | 🔵 Low | `src/__init__.py` purpose omitted | **Fixed** (documented in architecture & guide) |
| **P-15** | Docs | 🔵 Low | Premature log inspection during startup | **Fixed** (added startup-wait advisory) |
| **P-16** | Architecture | 🔵 Low | Ephemeral broker storage across restarts | **Fixed** (documented in README & guide) |
