# Q-Scheduler

A task scheduling service that dispatches work to remote quantum computing backends.

## Quick Start

### 1. Build the Docker image

```bash
docker build -t qscheduler .
```

### 2. Start the test backend

The test backend is a mock Python server that simulates a quantum machine backend.

```bash
cd test_backend
pip install -r requirements.txt
python server.py
```

The test backend listens on `http://localhost:8080`.

### 3. Configure and start the service

The service is configured via a TOML file. Use `test_backend/test.toml` to connect to the mock backend:

```bash
docker run -it --network host -v ./test_backend/test.toml:/config.toml qscheduler /config.toml
```

Or run directly with Cargo:

```bash
cargo run -p qscheduler -- test_backend/test.toml
```

The service starts on the port configured in the TOML file (default: `3000`).

## Swagger UI

Once the service is running, the interactive API docs are available at:

```
http://localhost:3000/swagger-ui
```
