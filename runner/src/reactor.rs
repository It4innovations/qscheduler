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

    let client = reqwest::Client::new();

    core.lock().unwrap().set_task_state(task_id, TaskState::Compiling);

    let compile_ok = client
        .post(format!("{backend_url}/task"))
        .body(payload.to_vec())
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);

    if !compile_ok {
        core.lock().unwrap().set_task_state(task_id, TaskState::Failed);
        return;
    }
    core.lock().unwrap().set_task_state(task_id, TaskState::Compiled);

    core.lock().unwrap().set_task_state(task_id, TaskState::Running);

    let start_ok = client
        .post(format!("{backend_url}/task/{}/start", task_id.as_u32()))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);

    if !start_ok {
        core.lock().unwrap().set_task_state(task_id, TaskState::Failed);
        return;
    }
    core.lock().unwrap().set_task_state(task_id, TaskState::Finished);
}
