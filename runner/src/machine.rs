use crate::UserId;
use crate::backend::{Backend, BackendConfig};
use crate::config::MachineConfiguration;
use crate::error::RunnerError;
use crate::id::define_id;
use crate::project::ProjectId;
use crate::session::{Session, SessionId};
use crate::suspension::{SuspendCancel, Suspension};
use crate::task::{Task, TaskConfig, TaskId};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::{Duration, Instant};
use typed_floats::PositiveFinite;

define_id!(MachineId);

const TASK_AVERAGE_ALPHA: f64 = 0.1;
const TASK_AVERAGE_PLACEHOLDER: f64 = 10.0;

#[derive(Clone, Copy)]
pub(crate) enum QueueItem {
    Task(TaskId),
    Session(SessionId),
}

#[derive(Clone)]
pub struct MachineConfig {
    pub name: String,
    pub queue_size: usize,
    pub decay_half_time: Duration,
    pub limit_check_interval: Duration,
    pub max_session_time: Duration,
    pub default_task_time_limit: Duration,
    pub max_clock_skew: Duration,
    pub pulse_access: bool,
    pub backend: BackendConfig,
}

impl MachineConfig {
    /// The configuration as stored in the DB.
    pub fn to_configuration(&self) -> MachineConfiguration {
        MachineConfiguration {
            name: self.name.clone(),
            queue_size: self.queue_size,
            limit_check_interval_ms: self.limit_check_interval.as_millis() as u32,
            max_session_time_ms: self.max_session_time.as_millis() as u64,
            default_task_time_limit_ms: self.default_task_time_limit.as_millis() as u64,
            max_clock_skew_ms: self.max_clock_skew.as_millis() as u64,
            decay_half_time_ms: self.decay_half_time.as_millis() as u64,
            pulse_access: self.pulse_access,
            backend: self.backend.clone(),
        }
    }
}

struct RunningSession {
    session_id: SessionId,
    queue: VecDeque<TaskId>,
}

/// A task that was already submitted to the backend before a restart and must be
/// re-attached to the launcher's backend on startup instead of being re-queued.
pub(crate) struct ResumeTask {
    pub task_id: TaskId,
    pub backend_id: String,
    pub payload: Bytes,
    pub cancel: bool,
    /// When the backend first reported the task running, if it did before the restart;
    /// the time limit deadline is rebuilt from it.
    pub started_at: Option<DateTime<Utc>>,
    pub time_limit: Duration,
    /// Project charged for the task's time (`None` for session tasks).
    pub project_id: Option<ProjectId>,
}

type Usage = PositiveFinite<f64>;

#[derive(Default)]
pub(crate) struct UserQueue {
    usage: Usage,
    average_usage: Option<Usage>,
    submitted_tasks: u32,
    queue: VecDeque<QueueItem>,
}

impl UserQueue {
    /// The usage that orders users for fair share: the time already charged, plus the expected
    /// time of the user's tasks in the backend, which are charged only when they finish.
    fn effective_usage(&self) -> Usage {
        let average = self
            .average_usage
            .map_or(TASK_AVERAGE_PLACEHOLDER, |a| a.get());
        Usage::new(self.usage.get() + average * self.submitted_tasks as f64).unwrap()
    }
}

pub(crate) struct Machine {
    machine_id: MachineId,
    queues: HashMap<UserId, UserQueue>,
    last_decay_update: Instant,
    running_session: Option<RunningSession>,
    config: MachineConfig,
    notifier: Arc<Notify>,
    backend: Arc<dyn Backend>,
    task_cancel_requests: Vec<TaskId>,
    session_cancel_requests: Vec<SessionId>,
    /// The active suspension, if any; new tasks and sessions are rejected while it is set.
    suspension: Option<Suspension>,
    /// Suspensions whose `cancel` policy the launcher has yet to apply to existing work, with
    /// the error to record on the failed tasks.
    suspend_cancel_requests: Vec<(SuspendCancel, String)>,
    /// Set while the machine is being reconfigured or removed; new tasks and sessions are
    /// rejected meanwhile.
    unavailable: bool,
}

/// A machine's configuration and state as reported by the API.
#[derive(Clone)]
pub struct MachineInfo {
    pub config: MachineConfig,
    pub suspension: Option<Suspension>,
}

impl Machine {
    pub fn new(machine_id: MachineId, config: MachineConfig, backend: Arc<dyn Backend>) -> Self {
        Machine {
            machine_id,
            queues: HashMap::new(),
            last_decay_update: Instant::now(),
            running_session: None,
            config,
            notifier: Arc::new(Notify::new()),
            backend,
            task_cancel_requests: Vec::new(),
            session_cancel_requests: Vec::new(),
            suspension: None,
            suspend_cancel_requests: Vec::new(),
            unavailable: false,
        }
    }

    pub fn info(&self) -> MachineInfo {
        MachineInfo {
            config: self.config.clone(),
            suspension: self.suspension.clone(),
        }
    }

    pub fn decay_update(&mut self, now: Instant) {
        let diff = (now - self.last_decay_update).as_secs_f64();
        if diff < 0.00001 {
            return;
        }
        self.last_decay_update = now;
        let decay_factor = (-diff / self.config.decay_half_time.as_secs_f64()).exp2();
        self.queues.retain(|_, qu| {
            qu.usage = Usage::new(qu.usage.get() * decay_factor).unwrap();
            !qu.queue.is_empty() || qu.submitted_tasks > 0 || qu.usage > 0.0001
        })
    }

    pub(crate) fn set_unavailable(&mut self, unavailable: bool) {
        self.unavailable = unavailable;
    }

    /// Takes over the users' decayed usage and task averages from `old`, a previous incarnation of
    /// this machine, so a reconfiguration doesn't reset fair share. Queues and counts of tasks in
    /// the backend are not inherited: `Core::restore` rebuilds them.
    pub(crate) fn inherit_usage(&mut self, old: &mut Machine) {
        old.decay_update(Instant::now());
        self.last_decay_update = old.last_decay_update;
        for (user_id, old_qu) in &old.queues {
            let new_qu = self.queues.entry(*user_id).or_default();
            new_qu.usage = old_qu.usage;
            new_qu.average_usage = old_qu.average_usage;
        }
    }

    /// Moves the not yet processed cancel requests of `old`, a previous incarnation of this
    /// machine, to this one, so cancels accepted during a reconfiguration are not lost.
    pub(crate) fn inherit_cancel_requests(&mut self, old: &mut Machine) {
        self.task_cancel_requests
            .append(&mut old.task_cancel_requests);
        self.session_cancel_requests
            .append(&mut old.session_cancel_requests);
    }

    pub fn config(&self) -> &MachineConfig {
        &self.config
    }

    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    pub fn suspension(&self) -> Option<&Suspension> {
        self.suspension.as_ref()
    }

    pub(crate) fn set_suspension(&mut self, suspension: Option<Suspension>) {
        self.suspension = suspension;
    }

    /// Fails with [`RunnerError::MachineSuspended`] if the machine is suspended, or with
    /// [`RunnerError::MachineUnavailable`] while it is being reconfigured or removed.
    pub fn check_not_suspended(&self) -> crate::Result<()> {
        if self.unavailable {
            return Err(RunnerError::MachineUnavailable(self.config.name.clone()));
        }
        match &self.suspension {
            Some(s) => Err(RunnerError::MachineSuspended {
                machine: self.config.name.clone(),
                message: s.message.clone(),
            }),
            None => Ok(()),
        }
    }

    pub fn queue_task(&mut self, task: &Task) -> crate::Result<()> {
        if let Some(s_id) = task.config().parent.session_id() {
            match &mut self.running_session {
                Some(s) if s_id == s.session_id => {
                    s.queue.push_back(task.id());
                }
                _ => return Err(RunnerError::NonRunningSession(s_id)),
            }
        } else {
            self.queues
                .entry(task.config().user_id)
                .or_default()
                .queue
                .push_back(QueueItem::Task(task.id()));
        }
        Ok(())
    }

    pub fn queue_session(&mut self, session: &Session) -> crate::Result<()> {
        self.queues
            .entry(session.config.owner_id)
            .or_default()
            .queue
            .push_back(QueueItem::Session(session.id()));
        Ok(())
    }

    pub fn wake_launcher(&self) {
        self.notifier.notify_one();
    }

    pub fn start_session(&mut self, session_id: SessionId) {
        assert!(self.running_session.is_none());
        self.running_session = Some(RunningSession {
            session_id,
            queue: VecDeque::new(),
        });
    }

    pub fn close_session(&mut self) {
        assert!(self.running_session.is_some());
        self.running_session = None;
    }

    #[inline]
    pub fn pop_session_task(&mut self) -> Option<TaskId> {
        self.running_session
            .as_mut()
            .and_then(|s| s.queue.pop_front())
    }

    #[inline]
    pub fn pop_queue_item(&mut self, allow_sessions: bool) -> Option<QueueItem> {
        let user_queue = self
            .queues
            .values_mut()
            .filter(|uq| !uq.queue.is_empty())
            .min_by_key(|uq| {
                let usage = uq.effective_usage();
                match uq.queue.front().unwrap() {
                    QueueItem::Task(task_id) => (usage, false, task_id.as_u64()),
                    QueueItem::Session(session_id) => (usage, true, session_id.as_u64()),
                }
            })?;
        if !allow_sessions && let Some(QueueItem::Session(_)) = user_queue.queue.front() {
            return None;
        }
        user_queue.queue.pop_front()
    }

    pub fn running_session_id(&self) -> Option<SessionId> {
        self.running_session.as_ref().map(|s| s.session_id)
    }

    pub fn notifier(&self) -> &Arc<Notify> {
        &self.notifier
    }

    pub(crate) fn request_task_cancel(&mut self, task_id: TaskId) {
        self.task_cancel_requests.push(task_id);
    }

    pub(crate) fn request_session_cancel(&mut self, session_id: SessionId) {
        self.session_cancel_requests.push(session_id);
    }

    pub(crate) fn request_suspend_cancel(&mut self, cancel: SuspendCancel, error: String) {
        self.suspend_cancel_requests.push((cancel, error));
    }

    pub(crate) fn take_suspend_cancel_requests(&mut self) -> Vec<(SuspendCancel, String)> {
        std::mem::take(&mut self.suspend_cancel_requests)
    }

    /// Removes all queued items, tasks first and then sessions, each ordered by ID; users' fair
    /// share state is kept.
    pub(crate) fn take_queued_items(&mut self) -> Vec<QueueItem> {
        let mut items: Vec<QueueItem> = self
            .queues
            .values_mut()
            .flat_map(|qu| qu.queue.drain(..))
            .collect();
        items.sort_unstable_by_key(|item| match item {
            QueueItem::Task(task_id) => (false, task_id.as_u64()),
            QueueItem::Session(session_id) => (true, session_id.as_u64()),
        });
        items
    }

    pub(crate) fn take_task_cancel_requests(&mut self) -> Vec<TaskId> {
        std::mem::take(&mut self.task_cancel_requests)
    }

    pub(crate) fn take_session_cancel_requests(&mut self) -> Vec<SessionId> {
        std::mem::take(&mut self.session_cancel_requests)
    }

    pub(crate) fn remove_queued_task(&mut self, task_id: TaskId, user_id: UserId) -> bool {
        if let Some(rs) = &mut self.running_session
            && let Some(pos) = rs.queue.iter().position(|t| *t == task_id)
        {
            rs.queue.remove(pos);
            return true;
        }
        if let Some(qu) = self.queues.get_mut(&user_id)
            && let Some(pos) = qu
                .queue
                .iter()
                .position(|item| matches!(item, QueueItem::Task(t) if *t == task_id))
        {
            qu.queue.remove(pos);
            return true;
        }
        false
    }

    pub(crate) fn remove_queued_session(&mut self, session_id: SessionId, user_id: UserId) -> bool {
        if let Some(qu) = self.queues.get_mut(&user_id)
            && let Some(pos) = qu
                .queue
                .iter()
                .position(|item| matches!(item, QueueItem::Session(s) if *s == session_id))
        {
            qu.queue.remove(pos);
            return true;
        }
        false
    }

    /// Charges the owner of a closed session for the time it held the machine. This goes to the
    /// usage only: the task average predicts the cost of individual tasks.
    pub fn record_closed_session(&mut self, owner_id: UserId, held_time: Duration) {
        let qu = self.queues.entry(owner_id).or_default();
        qu.usage = Usage::new(qu.usage.get() + held_time.as_secs_f64()).unwrap();
    }

    /// Records a task handed to the backend. Only tasks outside a session are under fair share.
    pub fn record_new_task_in_backend(&mut self, config: &TaskConfig) {
        if config.parent.session_id().is_some() {
            return;
        }
        let qu = self.queues.entry(config.user_id).or_default();
        qu.submitted_tasks += 1;
    }

    /// Records a task leaving the backend. Only tasks outside a session are under fair share, so
    /// a session task changes neither the user's usage nor their task average.
    pub fn record_finished_task_in_backend(&mut self, config: &TaskConfig, exec_time: Duration) {
        if config.parent.session_id().is_some() {
            return;
        }
        let qu = self.queues.get_mut(&config.user_id).unwrap();
        qu.submitted_tasks -= 1;
        if !exec_time.is_zero() {
            let exec_time = exec_time.as_secs_f64();
            qu.average_usage = Some(
                Usage::new(
                    qu.average_usage
                        .map(|a| {
                            TASK_AVERAGE_ALPHA * exec_time + (1.0 - TASK_AVERAGE_ALPHA) * a.get()
                        })
                        .unwrap_or(exec_time),
                )
                .unwrap(),
            );
            qu.usage = Usage::new(qu.usage.get() + exec_time).unwrap();
        }
    }
}

#[derive(Default)]
pub(crate) struct MachineMap {
    machines: HashMap<MachineId, Machine>,
    machines_by_name: HashMap<String, MachineId>,
}

impl MachineMap {
    pub fn find_machine(&self, machine_id: MachineId) -> crate::Result<&Machine> {
        self.machines
            .get(&machine_id)
            .ok_or_else(|| RunnerError::InvalidMachineId(machine_id))
    }
    pub fn find_machine_mut(&mut self, machine_id: MachineId) -> crate::Result<&mut Machine> {
        self.machines
            .get_mut(&machine_id)
            .ok_or_else(|| RunnerError::InvalidMachineId(machine_id))
    }
    pub fn get_machine(&self, machine_id: MachineId) -> &Machine {
        self.machines.get(&machine_id).unwrap()
    }
    pub fn get_machine_mut(&mut self, machine_id: MachineId) -> &mut Machine {
        self.machines.get_mut(&machine_id).unwrap()
    }
    pub fn insert(&mut self, machine: Machine) {
        let machine_id = machine.machine_id;
        self.machines_by_name
            .insert(machine.config.name.clone(), machine_id);
        self.machines.insert(machine_id, machine);
    }

    pub fn remove(&mut self, machine_id: MachineId) -> Option<Machine> {
        let machine = self.machines.remove(&machine_id)?;
        self.machines_by_name.remove(&machine.config.name);
        Some(machine)
    }

    pub fn find_machine_by_name(&self, name: &str) -> crate::Result<MachineId> {
        self.machines_by_name
            .get(name)
            .copied()
            .ok_or_else(|| RunnerError::InvalidMachineName(name.to_string()))
    }

    pub fn iter(&self) -> impl Iterator<Item = &Machine> {
        self.machines.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend_test::start_test_backend;
    use crate::task::{TaskKind, TaskParent};

    const HALF_TIME: Duration = Duration::from_secs(100);

    fn machine_with_half_time(decay_half_time: Duration) -> Machine {
        let (backend, _receiver) = start_test_backend();
        Machine::new(
            MachineId::try_from(1u32).unwrap(),
            MachineConfig {
                name: "m".to_string(),
                queue_size: 1,
                decay_half_time,
                limit_check_interval: Duration::from_secs(1),
                max_session_time: Duration::from_secs(1),
                default_task_time_limit: Duration::from_secs(1),
                max_clock_skew: Duration::ZERO,
                pulse_access: false,
                backend: BackendConfig::Test,
            },
            backend,
        )
    }

    fn machine() -> Machine {
        machine_with_half_time(HALF_TIME)
    }

    fn user(id: u32) -> UserId {
        UserId::try_from(id).unwrap()
    }

    fn task(id: u64) -> QueueItem {
        QueueItem::Task(TaskId::try_from(id).unwrap())
    }

    fn session(id: u64) -> QueueItem {
        QueueItem::Session(SessionId::try_from(id).unwrap())
    }

    /// A comparable form of a queue item.
    fn key(item: QueueItem) -> (&'static str, u64) {
        match item {
            QueueItem::Task(id) => ("task", id.as_u64()),
            QueueItem::Session(id) => ("session", id.as_u64()),
        }
    }

    fn enqueue(m: &mut Machine, u: UserId, item: QueueItem) {
        m.queues.entry(u).or_default().queue.push_back(item);
    }

    fn task_config(u: UserId, parent: TaskParent) -> TaskConfig {
        TaskConfig {
            machine_id: MachineId::try_from(1u32).unwrap(),
            parent,
            user_id: u,
            kind: TaskKind::default(),
            payload: Bytes::new(),
            time_limit: Duration::from_secs(1),
            notifier: None,
        }
    }

    fn project_task(u: UserId) -> TaskConfig {
        task_config(u, TaskParent::Project(ProjectId::try_from(1u32).unwrap()))
    }

    fn session_task(u: UserId) -> TaskConfig {
        task_config(u, TaskParent::Session(SessionId::try_from(1u64).unwrap()))
    }

    /// Hands a task outside a session to the backend.
    fn submit(m: &mut Machine, u: UserId) {
        m.record_new_task_in_backend(&project_task(u));
    }

    /// Finishes a task outside a session that is in the backend.
    fn finish(m: &mut Machine, u: UserId, seconds: f64) {
        m.record_finished_task_in_backend(&project_task(u), Duration::from_secs_f64(seconds));
    }

    /// Runs a whole task outside a session, charging `seconds` to the user.
    fn charge(m: &mut Machine, u: UserId, seconds: f64) {
        submit(m, u);
        finish(m, u, seconds);
    }

    fn usage(m: &Machine, u: UserId) -> f64 {
        m.queues[&u].usage.get()
    }

    fn in_backend(m: &Machine, u: UserId) -> u32 {
        m.queues[&u].submitted_tasks
    }

    fn average(m: &Machine, u: UserId) -> Option<f64> {
        m.queues[&u].average_usage.map(|a| a.get())
    }

    fn effective(m: &Machine, u: UserId) -> f64 {
        m.queues[&u].effective_usage().get()
    }

    fn pop(m: &mut Machine) -> Option<(&'static str, u64)> {
        m.pop_queue_item(true).map(key)
    }

    #[track_caller]
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "{actual} is not close to {expected}"
        );
    }

    // Decay

    #[test]
    fn decay_halves_usage_after_half_time() {
        let mut m = machine();
        charge(&mut m, user(1), 8.0);
        m.decay_update(m.last_decay_update + HALF_TIME);
        assert_close(usage(&m, user(1)), 4.0);
    }

    #[test]
    fn decay_follows_elapsed_half_times() {
        for (half_times, expected) in [(0.5, 8.0 / 2f64.sqrt()), (2.0, 2.0), (3.0, 1.0)] {
            let mut m = machine();
            charge(&mut m, user(1), 8.0);
            m.decay_update(m.last_decay_update + HALF_TIME.mul_f64(half_times));
            assert_close(usage(&m, user(1)), expected);
        }
    }

    #[test]
    fn decay_steps_compose() {
        let mut m = machine();
        charge(&mut m, user(1), 8.0);
        for _ in 0..4 {
            m.decay_update(m.last_decay_update + HALF_TIME / 2);
        }
        assert_close(usage(&m, user(1)), 2.0);
    }

    #[test]
    fn decay_uses_configured_half_time() {
        for (half_time, expected) in [
            (Duration::from_millis(1), 0.0),
            (Duration::from_secs(10), 4.0),
            (Duration::from_secs(5), 2.0),
            (Duration::from_secs(5400), 8.0 * (-10.0f64 / 5400.0).exp2()),
        ] {
            let mut m = machine_with_half_time(half_time);
            enqueue(&mut m, user(1), task(1));
            charge(&mut m, user(1), 8.0);
            m.decay_update(m.last_decay_update + Duration::from_secs(10));
            assert_close(usage(&m, user(1)), expected);
        }
    }

    #[test]
    fn decay_applies_to_all_users() {
        let mut m = machine();
        charge(&mut m, user(1), 8.0);
        charge(&mut m, user(2), 2.0);
        m.decay_update(m.last_decay_update + HALF_TIME);
        assert_close(usage(&m, user(1)), 4.0);
        assert_close(usage(&m, user(2)), 1.0);
    }

    #[test]
    fn decay_keeps_order_of_users() {
        let mut m = machine();
        charge(&mut m, user(1), 8.0);
        charge(&mut m, user(2), 2.0);
        enqueue(&mut m, user(1), task(1));
        enqueue(&mut m, user(2), task(2));
        m.decay_update(m.last_decay_update + HALF_TIME * 5);
        assert_eq!(pop(&mut m), Some(("task", 2)));
    }

    #[test]
    fn decay_ignores_tiny_time_steps() {
        let mut m = machine();
        charge(&mut m, user(1), 8.0);
        let before = m.last_decay_update;
        m.decay_update(before + Duration::from_micros(1));
        assert_eq!(m.last_decay_update, before);
        assert_eq!(usage(&m, user(1)), 8.0);
    }

    #[test]
    fn decay_does_not_lose_time_of_ignored_steps() {
        let mut m = machine();
        charge(&mut m, user(1), 8.0);
        let start = m.last_decay_update;
        m.decay_update(start + Duration::from_micros(1));
        m.decay_update(start + HALF_TIME);
        assert_close(usage(&m, user(1)), 4.0);
    }

    #[test]
    fn decay_moves_last_update() {
        let mut m = machine();
        let now = m.last_decay_update + HALF_TIME;
        m.decay_update(now);
        assert_eq!(m.last_decay_update, now);
    }

    #[test]
    fn decay_leaves_tasks_in_backend_and_average_alone() {
        let mut m = machine();
        charge(&mut m, user(1), 8.0);
        submit(&mut m, user(1));
        m.decay_update(m.last_decay_update + HALF_TIME * 10);
        assert_eq!(in_backend(&m, user(1)), 1);
        assert_eq!(average(&m, user(1)), Some(8.0));
    }

    #[test]
    fn decay_survives_very_long_idle_time() {
        let mut m = machine_with_half_time(Duration::from_millis(1));
        enqueue(&mut m, user(1), task(1));
        charge(&mut m, user(1), 1e12);
        // 2^-(10^10): the factor underflows to zero rather than panicking.
        m.decay_update(m.last_decay_update + Duration::from_secs(10_000_000));
        assert_eq!(usage(&m, user(1)), 0.0);
    }

    #[test]
    fn decay_handles_largest_half_time() {
        let mut m = machine_with_half_time(Duration::from_millis(u64::MAX));
        charge(&mut m, user(1), 8.0);
        m.decay_update(m.last_decay_update + HALF_TIME);
        assert_close(usage(&m, user(1)), 8.0);
    }

    #[test]
    fn decay_drops_idle_user_with_no_usage_left() {
        let mut m = machine();
        charge(&mut m, user(1), 0.001);
        m.decay_update(m.last_decay_update + HALF_TIME * 10);
        assert!(!m.queues.contains_key(&user(1)));
    }

    #[test]
    fn decay_keeps_idle_user_with_usage_left() {
        let mut m = machine();
        charge(&mut m, user(1), 100.0);
        m.decay_update(m.last_decay_update + HALF_TIME);
        assert!(m.queues.contains_key(&user(1)));
    }

    #[test]
    fn decay_keeps_user_with_queued_items() {
        let mut m = machine();
        enqueue(&mut m, user(1), task(1));
        m.decay_update(m.last_decay_update + HALF_TIME * 100);
        assert_eq!(pop(&mut m), Some(("task", 1)));
    }

    #[test]
    fn decay_keeps_user_with_running_task() {
        let mut m = machine();
        submit(&mut m, user(1));
        m.decay_update(m.last_decay_update + HALF_TIME * 100);
        assert!(m.queues.contains_key(&user(1)));
    }

    #[test]
    fn decay_drops_user_after_last_task_left_backend() {
        let mut m = machine();
        submit(&mut m, user(1));
        finish(&mut m, user(1), 0.0);
        m.decay_update(m.last_decay_update + HALF_TIME);
        assert!(!m.queues.contains_key(&user(1)));
    }

    // Picking the next item

    #[test]
    fn pop_from_empty_machine_returns_nothing() {
        assert!(pop(&mut machine()).is_none());
    }

    #[test]
    fn pop_prefers_lowest_usage() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        charge(&mut m, user(2), 3.0);
        charge(&mut m, user(3), 4.0);
        enqueue(&mut m, user(1), task(1));
        enqueue(&mut m, user(2), task(2));
        enqueue(&mut m, user(3), task(3));
        assert_eq!(pop(&mut m), Some(("task", 2)));
        assert_eq!(pop(&mut m), Some(("task", 3)));
        assert_eq!(pop(&mut m), Some(("task", 1)));
        assert_eq!(pop(&mut m), None);
    }

    #[test]
    fn pop_counts_tasks_in_backend_like_usage() {
        let mut m = machine();
        charge(&mut m, user(1), TASK_AVERAGE_PLACEHOLDER - 1.0);
        submit(&mut m, user(2));
        enqueue(&mut m, user(1), task(2));
        enqueue(&mut m, user(2), task(1));
        assert_eq!(pop(&mut m), Some(("task", 2)));
    }

    #[test]
    fn pop_adds_usage_and_tasks_in_backend_of_a_user() {
        let mut m = machine();
        charge(&mut m, user(1), 3.0);
        submit(&mut m, user(1));
        charge(&mut m, user(2), 5.0);
        enqueue(&mut m, user(1), task(1));
        enqueue(&mut m, user(2), task(2));
        assert_eq!(pop(&mut m), Some(("task", 2)));
    }

    #[test]
    fn pop_resolves_equal_usage_by_oldest_item() {
        let mut m = machine();
        enqueue(&mut m, user(1), task(2));
        enqueue(&mut m, user(2), task(1));
        assert_eq!(pop(&mut m), Some(("task", 1)));
        assert_eq!(pop(&mut m), Some(("task", 2)));
    }

    #[test]
    fn pop_resolves_equal_usage_for_task_before_session() {
        let mut m = machine();
        enqueue(&mut m, user(1), session(1));
        enqueue(&mut m, user(2), task(2));
        assert_eq!(pop(&mut m), Some(("task", 2)));
    }

    #[test]
    fn pop_keeps_queue_order_of_a_user() {
        let mut m = machine();
        enqueue(&mut m, user(1), task(3));
        enqueue(&mut m, user(1), task(1));
        enqueue(&mut m, user(1), session(2));
        assert_eq!(pop(&mut m), Some(("task", 3)));
        assert_eq!(pop(&mut m), Some(("task", 1)));
        assert_eq!(pop(&mut m), Some(("session", 2)));
    }

    #[test]
    fn pop_skips_users_with_empty_queue() {
        let mut m = machine();
        // Lowest usage, but nothing queued.
        charge(&mut m, user(1), 0.5);
        charge(&mut m, user(2), 5.0);
        enqueue(&mut m, user(2), task(1));
        assert_eq!(pop(&mut m), Some(("task", 1)));
    }

    #[test]
    fn pop_returns_session_when_allowed() {
        let mut m = machine();
        enqueue(&mut m, user(1), session(1));
        assert_eq!(m.pop_queue_item(true).map(key), Some(("session", 1)));
    }

    #[test]
    fn pop_leaves_next_session_when_not_allowed() {
        let mut m = machine();
        enqueue(&mut m, user(1), session(1));
        assert!(m.pop_queue_item(false).is_none());
        assert_eq!(m.pop_queue_item(true).map(key), Some(("session", 1)));
    }

    #[test]
    fn pop_does_not_let_others_overtake_next_session() {
        let mut m = machine();
        charge(&mut m, user(2), 5.0);
        enqueue(&mut m, user(1), session(1));
        enqueue(&mut m, user(2), task(2));
        assert!(m.pop_queue_item(false).is_none());
        assert_eq!(m.queues[&user(2)].queue.len(), 1);
    }

    #[test]
    fn pop_returns_task_of_lighter_user_before_session_when_not_allowed() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        enqueue(&mut m, user(1), session(1));
        enqueue(&mut m, user(2), task(2));
        assert_eq!(m.pop_queue_item(false).map(key), Some(("task", 2)));
    }

    #[test]
    fn pop_does_not_change_fair_share_state() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        submit(&mut m, user(1));
        enqueue(&mut m, user(1), task(1));
        pop(&mut m);
        assert_eq!(usage(&m, user(1)), 5.0);
        assert_eq!(in_backend(&m, user(1)), 1);
        assert_eq!(average(&m, user(1)), Some(5.0));
    }

    // Tasks in the backend and usage

    #[test]
    fn new_task_creates_user() {
        let mut m = machine();
        submit(&mut m, user(1));
        assert_eq!(in_backend(&m, user(1)), 1);
        assert_eq!(usage(&m, user(1)), 0.0);
        assert_eq!(average(&m, user(1)), None);
    }

    #[test]
    fn tasks_in_backend_add_up() {
        let mut m = machine();
        submit(&mut m, user(1));
        submit(&mut m, user(1));
        assert_eq!(in_backend(&m, user(1)), 2);
    }

    #[test]
    fn finished_task_leaves_backend_and_charges_time() {
        let mut m = machine();
        submit(&mut m, user(1));
        submit(&mut m, user(1));
        finish(&mut m, user(1), 2.0);
        assert_eq!(in_backend(&m, user(1)), 1);
        assert_eq!(usage(&m, user(1)), 2.0);
    }

    #[test]
    fn finished_tasks_add_to_earlier_usage() {
        let mut m = machine();
        charge(&mut m, user(1), 2.0);
        charge(&mut m, user(1), 3.0);
        assert_eq!(usage(&m, user(1)), 5.0);
    }

    #[test]
    fn users_do_not_affect_each_other() {
        let mut m = machine();
        submit(&mut m, user(1));
        charge(&mut m, user(2), 1.0);
        assert_eq!(in_backend(&m, user(1)), 1);
        assert_eq!(usage(&m, user(1)), 0.0);
        assert_eq!(average(&m, user(1)), None);
    }

    // Task average

    #[test]
    fn first_finished_task_sets_average() {
        let mut m = machine();
        charge(&mut m, user(1), 10.0);
        assert_eq!(average(&m, user(1)), Some(10.0));
    }

    #[test]
    fn later_finished_tasks_blend_into_average() {
        let mut m = machine();
        charge(&mut m, user(1), 10.0);
        charge(&mut m, user(1), 20.0);
        let expected = TASK_AVERAGE_ALPHA * 20.0 + (1.0 - TASK_AVERAGE_ALPHA) * 10.0;
        assert_close(average(&m, user(1)).unwrap(), expected);
    }

    #[test]
    fn task_without_exec_time_changes_neither_average_nor_usage() {
        let mut m = machine();
        charge(&mut m, user(1), 0.0);
        assert_eq!(average(&m, user(1)), None);
        charge(&mut m, user(1), 10.0);
        charge(&mut m, user(1), 0.0);
        assert_eq!(average(&m, user(1)), Some(10.0));
        assert_eq!(usage(&m, user(1)), 10.0);
        assert_eq!(in_backend(&m, user(1)), 0);
    }

    // Effective usage

    #[test]
    fn effective_usage_of_idle_user_is_usage() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        assert_eq!(effective(&m, user(1)), 5.0);
    }

    #[test]
    fn effective_usage_prices_tasks_with_placeholder_before_any_average() {
        let mut m = machine();
        submit(&mut m, user(1));
        submit(&mut m, user(1));
        assert_eq!(effective(&m, user(1)), 2.0 * TASK_AVERAGE_PLACEHOLDER);
    }

    #[test]
    fn effective_usage_prices_tasks_with_average() {
        let mut m = machine();
        charge(&mut m, user(1), 3.0);
        submit(&mut m, user(1));
        submit(&mut m, user(1));
        assert_eq!(effective(&m, user(1)), 3.0 + 2.0 * 3.0);
    }

    // Sessions

    #[test]
    fn session_task_does_not_create_user() {
        let mut m = machine();
        m.record_new_task_in_backend(&session_task(user(1)));
        m.record_finished_task_in_backend(&session_task(user(1)), Duration::from_secs(2));
        assert!(!m.queues.contains_key(&user(1)));
    }

    #[test]
    fn session_task_leaves_fair_share_state_alone() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        submit(&mut m, user(1));
        m.record_new_task_in_backend(&session_task(user(1)));
        assert_eq!(in_backend(&m, user(1)), 1);
        m.record_finished_task_in_backend(&session_task(user(1)), Duration::from_secs(2));
        assert_eq!(in_backend(&m, user(1)), 1);
        assert_eq!(usage(&m, user(1)), 5.0);
        assert_eq!(average(&m, user(1)), Some(5.0));
    }

    #[test]
    fn closed_session_charges_usage_but_not_average() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        m.record_closed_session(user(1), Duration::from_secs(100));
        assert_eq!(usage(&m, user(1)), 105.0);
        assert_eq!(average(&m, user(1)), Some(5.0));
        assert_eq!(in_backend(&m, user(1)), 0);
    }

    #[test]
    fn closed_session_of_unknown_user_creates_entry() {
        let mut m = machine();
        m.record_closed_session(user(1), Duration::from_secs(2));
        assert_eq!(usage(&m, user(1)), 2.0);
        assert_eq!(average(&m, user(1)), None);
    }

    // Removing queued items

    #[test]
    fn take_queued_items_orders_tasks_before_sessions_by_id() {
        let mut m = machine();
        enqueue(&mut m, user(1), session(5));
        enqueue(&mut m, user(1), task(4));
        enqueue(&mut m, user(2), task(1));
        enqueue(&mut m, user(2), session(2));
        enqueue(&mut m, user(3), task(3));
        let items: Vec<_> = m.take_queued_items().into_iter().map(key).collect();
        assert_eq!(
            items,
            [
                ("task", 1),
                ("task", 3),
                ("task", 4),
                ("session", 2),
                ("session", 5)
            ]
        );
    }

    #[test]
    fn take_queued_items_leaves_queues_empty() {
        let mut m = machine();
        enqueue(&mut m, user(1), task(1));
        assert_eq!(m.take_queued_items().len(), 1);
        assert!(m.take_queued_items().is_empty());
        assert!(pop(&mut m).is_none());
    }

    #[test]
    fn take_queued_items_keeps_fair_share_state() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        submit(&mut m, user(1));
        enqueue(&mut m, user(1), task(1));
        m.take_queued_items();
        assert_eq!(usage(&m, user(1)), 5.0);
        assert_eq!(in_backend(&m, user(1)), 1);
    }

    #[test]
    fn take_queued_items_keeps_users_without_items() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        assert!(m.take_queued_items().is_empty());
        assert_eq!(usage(&m, user(1)), 5.0);
    }

    #[test]
    fn removing_queued_items_keeps_fair_share_state() {
        let mut m = machine();
        charge(&mut m, user(1), 5.0);
        enqueue(&mut m, user(1), task(1));
        enqueue(&mut m, user(1), session(1));
        assert!(m.remove_queued_task(TaskId::try_from(1u64).unwrap(), user(1)));
        assert!(m.remove_queued_session(SessionId::try_from(1u64).unwrap(), user(1)));
        assert_eq!(usage(&m, user(1)), 5.0);
        assert!(pop(&mut m).is_none());
    }

    #[test]
    fn removing_unknown_queued_items_finds_nothing() {
        let mut m = machine();
        enqueue(&mut m, user(1), task(1));
        assert!(!m.remove_queued_task(TaskId::try_from(2u64).unwrap(), user(1)));
        assert!(!m.remove_queued_task(TaskId::try_from(1u64).unwrap(), user(2)));
        assert!(!m.remove_queued_session(SessionId::try_from(1u64).unwrap(), user(1)));
        assert_eq!(pop(&mut m), Some(("task", 1)));
    }

    // Reconfiguration

    #[test]
    fn inherit_usage_takes_over_usage() {
        let mut old = machine();
        charge(&mut old, user(1), 5.0);
        charge(&mut old, user(2), 3.0);
        let mut new = machine();
        new.inherit_usage(&mut old);
        assert!((usage(&new, user(1)) - 5.0).abs() < 0.01);
        assert!((usage(&new, user(2)) - 3.0).abs() < 0.01);
    }

    #[test]
    fn inherit_usage_decays_with_old_half_time_first() {
        let mut old = machine_with_half_time(Duration::from_secs(10));
        charge(&mut old, user(1), 8.0);
        old.last_decay_update = Instant::now() - Duration::from_secs(10);
        let mut new = machine_with_half_time(Duration::from_secs(1000));
        new.inherit_usage(&mut old);
        assert!((usage(&new, user(1)) - 4.0).abs() < 0.01);
        assert_eq!(new.last_decay_update, old.last_decay_update);
    }

    #[test]
    fn inherit_usage_then_decays_with_new_half_time() {
        let mut old = machine_with_half_time(Duration::from_secs(1000));
        charge(&mut old, user(1), 8.0);
        let mut new = machine_with_half_time(Duration::from_secs(1));
        new.inherit_usage(&mut old);
        let before = usage(&new, user(1));
        new.decay_update(new.last_decay_update + Duration::from_secs(1));
        assert_close(usage(&new, user(1)), before / 2.0);
    }

    #[test]
    fn inherit_usage_takes_over_task_average() {
        let mut old = machine();
        charge(&mut old, user(1), 5.0);
        let mut new = machine();
        new.inherit_usage(&mut old);
        assert_eq!(average(&new, user(1)), Some(5.0));
    }

    #[test]
    fn inherit_usage_skips_tasks_in_backend_and_queues() {
        let mut old = machine();
        charge(&mut old, user(1), 5.0);
        submit(&mut old, user(1));
        enqueue(&mut old, user(1), task(1));
        let mut new = machine();
        new.inherit_usage(&mut old);
        assert_eq!(in_backend(&new, user(1)), 0);
        assert!(pop(&mut new).is_none());
    }

    #[test]
    fn inherit_usage_keeps_what_restore_rebuilds_afterwards() {
        let mut old = machine();
        charge(&mut old, user(1), 5.0);
        let mut new = machine();
        new.inherit_usage(&mut old);
        submit(&mut new, user(1));
        enqueue(&mut new, user(1), task(1));
        assert!((usage(&new, user(1)) - 5.0).abs() < 0.01);
        assert_eq!(in_backend(&new, user(1)), 1);
        assert_eq!(pop(&mut new), Some(("task", 1)));
    }

    #[test]
    fn inherit_usage_does_not_create_users_with_nothing_left() {
        let mut old = machine_with_half_time(Duration::from_secs(1));
        charge(&mut old, user(1), 0.001);
        old.last_decay_update = Instant::now() - Duration::from_secs(100);
        let mut new = machine();
        new.inherit_usage(&mut old);
        assert!(!new.queues.contains_key(&user(1)));
    }

    // Configuration

    #[test]
    fn stored_configuration_keeps_decay_half_time() {
        let m = machine_with_half_time(Duration::from_millis(1234));
        assert_eq!(m.config().to_configuration().decay_half_time_ms, 1234);
    }
}
