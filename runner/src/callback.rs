use crate::config::NotifierConfiguration;
use crate::error::RunnerError;
use crate::id::define_id;
use crate::session::SessionInfo;
use crate::task::TaskInfo;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

/// Maximum length of a notifier's name (the key tasks and sessions refer to it by).
pub const MAX_NOTIFIER_NAME_LEN: usize = 512;

define_id!(NotifierId);

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
    pub url: String,
    pub has_token: bool,
    pub sender: UnboundedSender<NotifyEvent>,
}

/// A registered notifier as listed by the API (without its token).
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct NotifierInfo {
    pub name: String,
    pub url: String,
    /// Whether event payloads include a token.
    pub has_token: bool,
}

/// The registered notifiers, each with its own worker draining its event queue.
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
            map.add(notifier_id, config);
        }
        map
    }

    /// Registers a notifier and starts its worker thread.
    pub fn add(&mut self, notifier_id: NotifierId, config: NotifierConfiguration) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<NotifyEvent>();
        let notifier = Notifier {
            name: config.name.clone(),
            url: config.url.clone(),
            has_token: config.token.is_some(),
            sender,
        };
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime for notifier")
                .block_on(notify_worker(receiver, config));
        });
        self.notifiers_by_name
            .insert(notifier.name.clone(), notifier_id);
        self.notifiers.insert(notifier_id, notifier);
    }

    /// Unregisters a notifier. Its worker still delivers the events already queued, then exits;
    /// tasks and sessions that use it stop sending notifications.
    pub fn remove(&mut self, name: &str) -> Option<NotifierId> {
        let notifier_id = self.notifiers_by_name.remove(name)?;
        self.notifiers.remove(&notifier_id);
        Some(notifier_id)
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

    /// All notifiers, sorted by name.
    pub fn list(&self) -> Vec<NotifierInfo> {
        let mut list: Vec<NotifierInfo> = self
            .notifiers
            .values()
            .map(|n| NotifierInfo {
                name: n.name.clone(),
                url: n.url.clone(),
                has_token: n.has_token,
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
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

#[cfg(test)]
mod tests {
    use super::{NotifierId, NotifierMap};
    use crate::config::NotifierConfiguration;

    fn config(name: &str, token: Option<&str>) -> NotifierConfiguration {
        NotifierConfiguration {
            name: name.to_string(),
            url: format!("http://localhost/{name}"),
            token: token.map(str::to_string),
        }
    }

    #[test]
    fn add_and_remove_notifiers() {
        let id1 = NotifierId::try_from(1u32).unwrap();
        let id2 = NotifierId::try_from(2u32).unwrap();
        let mut map = NotifierMap::start(vec![(id1, config("b", Some("t")))]);
        map.add(id2, config("a", None));
        assert_eq!(map.find_notifier_by_name("a").unwrap(), id2);
        let names: Vec<_> = map
            .list()
            .into_iter()
            .map(|n| (n.name, n.has_token))
            .collect();
        assert_eq!(
            names,
            vec![("a".to_string(), false), ("b".to_string(), true)]
        );

        assert_eq!(map.remove("b"), Some(id1));
        assert_eq!(map.remove("b"), None);
        assert!(map.find_notifier_by_name("b").is_err());
        assert!(map.get(Some(id1)).is_none());
        assert_eq!(map.name(Some(id2)), Some("a".to_string()));
    }
}
