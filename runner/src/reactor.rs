use std::sync::{Arc, Mutex};
use crate::core::Core;
use crate::task::{TaskConfig, TaskId, TaskState};

pub fn submit_task(core: Arc<Mutex<Core>>, config: TaskConfig) -> TaskId {
    let task_id = core.lock().unwrap().add_task(config);
    tokio::spawn(start_task(Arc::clone(&core), task_id));
    task_id
}

async fn start_task(core: Arc<Mutex<Core>>, task_id: TaskId) {
    let (backend_url, payload) = {
        let locked = core.lock().unwrap();
        let backend_url = locked.backend_url().to_owned();
        let payload = locked.get_task(task_id).config().payload.clone();
        (backend_url, payload)
    };

    core.lock().unwrap().set_task_state(task_id, TaskState::Running);

    let client = reqwest::Client::new();
    let state = match run_task(&client, &backend_url, payload).await {
        Ok(()) => TaskState::Finished,
        Err(()) => TaskState::Failed,
    };
    core.lock().unwrap().set_task_state(task_id, state);
}

async fn run_task(client: &reqwest::Client, backend_url: &str, payload: Arc<[u8]>) -> Result<(), ()> {
    // Spawn a worker instance from the gateway
    let worker_resp = client.post(format!("{backend_url}/worker"))
        .send().await
        .map_err(|e| tracing::error!("POST /worker failed: {e}"))?;
    let status = worker_resp.status();
    let worker_json: serde_json::Value = worker_resp.json().await
        .map_err(|e| tracing::error!("POST /worker ({status}): failed to parse response: {e}"))?;
    let worker_url = worker_json["url"].as_str()
        .ok_or_else(|| tracing::error!("POST /worker: missing 'url' in response: {worker_json}"))?.to_owned();
    tracing::debug!(worker_url, "worker spawned");

    // Fetch calibration ID from the worker's architecture endpoint
    let arch_resp = client.get(format!("{worker_url}/architecture"))
        .send().await
        .map_err(|e| tracing::error!("GET /architecture failed: {e}"))?;
    let status = arch_resp.status();
    let arch_json: serde_json::Value = arch_resp.json().await
        .map_err(|e| tracing::error!("GET /architecture ({status}): failed to parse response: {e}"))?;
    let calibration_id = arch_json["calibration_id"].as_str()
        .ok_or_else(|| tracing::error!("GET /architecture: missing 'calibration_id' in response: {arch_json}"))?.to_owned();
    tracing::debug!(calibration_id, "got calibration id");

    // Submit circuit as multipart/form-data
    let qasm_part = reqwest::multipart::Part::bytes(payload.to_vec())
        .file_name("circuit.qasm")
        .mime_str("text/plain").map_err(|e| tracing::error!("failed to build multipart part: {e}"))?;
    let form = reqwest::multipart::Form::new()
        .text("calibration_id", calibration_id)
        .text("execution_args", "{}")
        .part("openqasm", qasm_part);

    let task_resp = client.post(format!("{worker_url}/task"))
        .multipart(form)
        .send().await
        .map_err(|e| tracing::error!("POST /task failed: {e}"))?;
    let status = task_resp.status();
    if !status.is_success() {
        let body = task_resp.text().await.unwrap_or_default();
        tracing::error!("POST /task returned {status}: {body}");
        return Err(());
    }
    let task_json: serde_json::Value = task_resp.json().await
        .map_err(|e| tracing::error!("POST /task ({status}): failed to parse response: {e}"))?;
    let qtask_id = task_json["qtask_id"].as_str()
        .ok_or_else(|| tracing::error!("POST /task: missing 'qtask_id' in response: {task_json}"))?.to_owned();
    tracing::debug!(qtask_id, "task registered on worker");

    // Start execution by sending the task UUID as raw bytes
    let uuid_bytes = parse_uuid_bytes(&qtask_id)
        .ok_or_else(|| tracing::error!("failed to parse qtask_id as UUID: {qtask_id}"))?;
    let start_resp = client.post(format!("{worker_url}/task/start"))
        .body(uuid_bytes)
        .send().await
        .map_err(|e| tracing::error!("POST /task/start failed: {e}"))?;
    let status = start_resp.status();
    if !status.is_success() {
        let body = start_resp.text().await.unwrap_or_default();
        tracing::error!("POST /task/start returned {status}: {body}");
        return Err(());
    }
    tracing::debug!("task completed successfully");

    Ok(())
}

fn parse_uuid_bytes(uuid_str: &str) -> Option<Vec<u8>> {
    let hex: String = uuid_str.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    (0..16).map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()).collect()
}
