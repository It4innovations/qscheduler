use crate::config::NotifierConfiguration;
use crate::error::RunnerError;
use crate::session::SessionInfo;
use crate::task::TaskInfo;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

/// Maximum length of a notifier's name (the key tasks and sessions refer to it by).
pub const MAX_NOTIFIER_NAME_LEN: usize = 12;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct NotifierId(u32);

impl Display for NotifierId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for NotifierId {
    fn from(v: u32) -> Self {
        NotifierId(v)
    }
}

impl NotifierId {
    #[inline]
    pub fn as_i32(&self) -> i32 {
        self.0 as i32
    }
}

/// Checks a notifier name: non-empty and at most [`MAX_NOTIFIER_NAME_LEN`] characters.
pub(crate) fn validate_notifier_name(name: &str) -> crate::Result<()> {
    let len = name.chars().count();
    if len == 0 || len > MAX_NOTIFIER_NAME_LEN {
        return Err(RunnerError::InvalidNotifierName(format!(
            "notifier name must have 1 to {MAX_NOTIFIER_NAME_LEN} characters, got {len}"
        )));
    }
    Ok(())
}

pub(crate) struct Notifier {
    pub name: String,
    pub sender: UnboundedSender<NotifyEvent>,
}

/// The notifiers loaded at startup, each with its own worker draining its event queue. Fixed for
/// the lifetime of the service: adding or removing a notifier takes effect after a restart.
#[derive(Default)]
pub(crate) struct NotifierMap {
    notifiers: HashMap<NotifierId, Notifier>,
    notifiers_by_name: HashMap<String, NotifierId>,
}

impl NotifierMap {
    /// Starts one worker thread per notifier.
    pub fn start(configs: Vec<(NotifierId, NotifierConfiguration)>) -> Self {
        let mut map = NotifierMap::default();
        for (notifier_id, config) in configs {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<NotifyEvent>();
            let name = config.name.clone();
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime for notifier")
                    .block_on(notify_worker(receiver, config));
            });
            map.notifiers_by_name.insert(name.clone(), notifier_id);
            map.notifiers.insert(notifier_id, Notifier { name, sender });
        }
        map
    }

    pub fn find_notifier_by_name(&self, name: &str) -> crate::Result<NotifierId> {
        self.notifiers_by_name
            .get(name)
            .copied()
            .ok_or_else(|| RunnerError::UnknownNotifier(name.to_string()))
    }

    /// The notifier of a task or session, `None` if it has none.
    pub fn get(&self, notifier_id: Option<NotifierId>) -> Option<&Notifier> {
        notifier_id.and_then(|id| self.notifiers.get(&id))
    }

    /// The name of a task's or session's notifier, as reported in its public info.
    pub fn name(&self, notifier_id: Option<NotifierId>) -> Option<String> {
        self.get(notifier_id).map(|n| n.name.clone())
    }
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "lowercase")]
pub(crate) enum NotifyEvent {
    Task { task: TaskInfo },
    Session { session: SessionInfo },
}

impl NotifyEvent {
    /// The event's subject as `(task_id, session_id)`, exactly one of them set; logged as
    /// separate fields (tracing omits a `None` field).
    fn subject_ids(&self) -> (Option<u64>, Option<u64>) {
        match self {
            NotifyEvent::Task { task } => (Some(task.id), None),
            NotifyEvent::Session { session } => (None, Some(session.id)),
        }
    }
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
    config: NotifierConfiguration,
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
                    let reason = failure_reason(&result);
                    if fail_count >= MAX_NOTIFY_FAILS {
                        let (task_id, session_id) = evt.event.subject_ids();
                        warn!(notifier = %config.name, %reason, task_id, session_id, "discarding notification after max failures");
                        break;
                    }
                    debug!(notifier = %config.name, %reason, fail_count, "notify attempt failed, retrying");
                    sleep_until(Instant::now() + delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}

/// Short description of a failed delivery attempt, e.g. `HTTP 503` or `cannot connect`.
fn failure_reason(result: &reqwest::Result<reqwest::Response>) -> String {
    match result {
        Ok(resp) => format!("HTTP {}", resp.status().as_u16()),
        Err(e) if e.is_connect() => "cannot connect".to_string(),
        Err(e) if e.is_timeout() => "timeout".to_string(),
        Err(e) if e.is_builder() => "invalid request (bad URL?)".to_string(),
        Err(_) => "request failed".to_string(),
    }
}
