use crate::TaskId;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{Instant, sleep_until};
use tracing::debug;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NotifyConfig {
    pub url: String,
    pub token: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum NotifyTaskState {
    Finished,
    Failed,
    Cancelled,
}

#[derive(Serialize)]
pub(crate) struct NotifyEvent {
    pub task_id: TaskId,
    pub state: NotifyTaskState,
}

#[derive(Serialize)]
pub(crate) struct NotifyEventWithToken<'a> {
    #[serde(flatten)]
    pub event: NotifyEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<&'a str>,
}

const MAX_NOTIFY_FAILS: u32 = 16;

pub(crate) async fn notify_worker(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<NotifyEvent>,
    config: NotifyConfig,
) {
    let client = reqwest::Client::new();
    while let Some(event) = receiver.recv().await {
        let mut fail_count = 0u32;
        let mut delay = Duration::from_millis(100);
        let evt = NotifyEventWithToken {
            event,
            token: config.token.as_deref(),
        };
        loop {
            match client.post(&config.url).json(&evt).send().await {
                Ok(resp) if resp.status().is_success() => break,
                _ => {
                    fail_count += 1;
                    if fail_count >= MAX_NOTIFY_FAILS {
                        debug!(task = %evt.event.task_id, "Discarding notification after max failures");
                        break;
                    }
                    sleep_until(Instant::now() + delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}
