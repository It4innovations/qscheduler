use crate::id::define_id;
use std::collections::HashMap;
use std::time::{Duration, Instant};

define_id!(UserId);

/// Time consumed by a user on one combination of a project and a machine.
#[derive(Debug, sqlx::FromRow, serde::Serialize, utoipa::ToSchema)]
pub struct UserConsumption {
    /// Name of the project the time was charged to.
    pub project: String,
    /// Name of the machine the time was spent on.
    pub machine: String,
    /// Confirmed time consumed, in milliseconds.
    pub consumed_ms: i64,
}

#[derive(Debug)]
pub struct User {
    pub name: String,
    /// Number of the user's tasks in a non-final state; all of them are resident in memory.
    pub active_tasks: u32,
    /// Total size of the payloads of the user's tasks in a non-final state, in bytes.
    pub active_payload_bytes: u64,
    /// Since when the user has had no active task (or was last looked up); drives its eviction
    /// from memory.
    idle_since: Instant,
}

/// Users with an active task (and recently used ones). Every non-final task's user is here;
/// idle users are evicted and reloaded from the DB on demand.
#[derive(Default)]
pub(crate) struct UserMap {
    users: HashMap<UserId, User>,
    users_by_name: HashMap<String, UserId>,
}

impl UserMap {
    pub fn get_user(&self, id: UserId) -> &User {
        self.users.get(&id).unwrap()
    }

    pub fn find_user(&self, id: UserId) -> Option<&User> {
        self.users.get(&id)
    }

    /// Looks up a user by name without marking it as recently used.
    pub fn find_by_name(&self, name: &str) -> Option<&User> {
        self.users.get(self.users_by_name.get(name)?)
    }

    /// Looks up a user by name, marking it as recently used so it isn't evicted before the
    /// caller's task is added.
    pub fn touch_by_name(&mut self, name: &str) -> Option<UserId> {
        let id = *self.users_by_name.get(name)?;
        let user = self.users.get_mut(&id).unwrap();
        if user.active_tasks == 0 {
            user.idle_since = Instant::now();
        }
        Some(id)
    }

    /// Adds a user loaded from the DB, unless it is already present; either way it is marked as
    /// recently used.
    pub fn insert(&mut self, id: UserId, name: &str) {
        match self.users.get_mut(&id) {
            Some(user) => {
                if user.active_tasks == 0 {
                    user.idle_since = Instant::now();
                }
            }
            None => {
                self.users_by_name.insert(name.to_string(), id);
                self.users.insert(
                    id,
                    User {
                        name: name.to_string(),
                        active_tasks: 0,
                        active_payload_bytes: 0,
                        idle_since: Instant::now(),
                    },
                );
            }
        }
    }

    /// Counts a new active task of the user; fails if the user is not in memory.
    pub fn task_activated(&mut self, id: UserId, payload_size: u64) -> crate::Result<()> {
        let user = self.users.get_mut(&id).ok_or_else(|| {
            crate::error::RunnerError::GenericError(format!("user {id} is not loaded"))
        })?;
        user.active_tasks += 1;
        user.active_payload_bytes += payload_size;
        Ok(())
    }

    /// Uncounts an active task of the user, which reached a final state (or left memory).
    pub fn task_deactivated(&mut self, id: UserId, payload_size: u64) {
        let user = self.users.get_mut(&id).unwrap();
        user.active_tasks -= 1;
        user.active_payload_bytes -= payload_size;
        if user.active_tasks == 0 {
            user.idle_since = Instant::now();
        }
    }

    /// Removes users without an active task that have been idle for at least `retention`
    /// as of `now`. With the same `now` and `retention` as `TaskMap::evict_finished`, no user
    /// of a task still in memory is removed: a user goes idle no earlier than its last task
    /// finishes.
    pub fn evict_idle(&mut self, now: Instant, retention: Duration) -> usize {
        let before = self.users.len();
        let users_by_name = &mut self.users_by_name;
        self.users.retain(|_, u| {
            let keep =
                u.active_tasks > 0 || now.saturating_duration_since(u.idle_since) < retention;
            if !keep {
                users_by_name.remove(&u.name);
            }
            keep
        });
        before - self.users.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uid(v: u32) -> UserId {
        UserId::try_from(v).unwrap()
    }

    #[test]
    fn insert_and_lookup() {
        let mut users = UserMap::default();
        assert!(users.touch_by_name("alice").is_none());
        users.insert(uid(1), "alice");
        users.insert(uid(1), "alice");
        assert_eq!(users.touch_by_name("alice"), Some(uid(1)));
        assert_eq!(users.get_user(uid(1)).name, "alice");
        assert_eq!(users.get_user(uid(1)).active_tasks, 0);
    }

    #[test]
    fn activation_requires_loaded_user() {
        let mut users = UserMap::default();
        assert!(users.task_activated(uid(1), 0).is_err());
    }

    #[test]
    fn tracks_active_payload_bytes() {
        let mut users = UserMap::default();
        users.insert(uid(1), "alice");
        users.insert(uid(2), "bob");
        users.task_activated(uid(1), 100).unwrap();
        users.task_activated(uid(1), 20).unwrap();
        users.task_activated(uid(2), 7).unwrap();
        assert_eq!(users.get_user(uid(1)).active_payload_bytes, 120);
        assert_eq!(users.get_user(uid(2)).active_payload_bytes, 7);

        // Re-inserting a loaded user keeps its counters.
        users.insert(uid(1), "alice");
        assert_eq!(users.get_user(uid(1)).active_payload_bytes, 120);

        users.task_deactivated(uid(1), 100);
        assert_eq!(users.get_user(uid(1)).active_payload_bytes, 20);
        users.task_deactivated(uid(1), 20);
        assert_eq!(users.get_user(uid(1)).active_payload_bytes, 0);
        assert_eq!(users.find_by_name("bob").unwrap().active_payload_bytes, 7);
    }

    #[test]
    fn evicts_only_idle_users() {
        let mut users = UserMap::default();
        users.insert(uid(1), "alice");
        users.insert(uid(2), "bob");
        users.task_activated(uid(1), 0).unwrap();
        let retention = Duration::from_secs(10);
        let later = Instant::now() + retention;

        // Within the retention period nobody goes.
        assert_eq!(users.evict_idle(Instant::now(), retention), 0);
        // After it, only the user without an active task goes.
        assert_eq!(users.evict_idle(later, retention), 1);
        assert!(users.find_user(uid(1)).is_some());
        assert!(users.touch_by_name("bob").is_none());

        // Its last task finishing starts the idle period.
        users.task_activated(uid(1), 0).unwrap();
        users.task_deactivated(uid(1), 0);
        assert_eq!(users.get_user(uid(1)).active_tasks, 1);
        users.task_deactivated(uid(1), 0);
        assert_eq!(users.evict_idle(Instant::now(), retention), 0);
        assert_eq!(users.evict_idle(Instant::now() + retention, retention), 1);
        assert!(users.touch_by_name("alice").is_none());
    }
}
