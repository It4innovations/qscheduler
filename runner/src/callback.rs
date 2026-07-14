use crate::session::SessionInfo;
use crate::task::TaskInfo;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NotifyConfig {
    pub url: String,
    pub token: Option<String>,
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "lowercase")]
pub(crate) enum NotifyEvent {
    Task { task: TaskInfo },
    Session { session: SessionInfo },
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
                result => {
                    fail_count += 1;
                    if fail_count >= MAX_NOTIFY_FAILS {
                        match &evt.event {
                            NotifyEvent::Task { task } => {
                                warn!(task_id = %task.id, "discarding notification after max failures")
                            }
                            NotifyEvent::Session { session } => {
                                warn!(session_id = %session.id, "discarding notification after max failures")
                            }
                        }
                        break;
                    }
                    debug!(?result, fail_count, "notify attempt failed, retrying");
                    sleep_until(Instant::now() + delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}
