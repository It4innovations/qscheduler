use crate::id::define_id;
use std::collections::HashMap;
use std::time::Duration;

define_id!(ProjectId);

#[derive(Debug, Clone)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub active: bool,
    /// Confirmed consumption: finished tasks and checkpointed session time, i.e. what the DB
    /// holds (or is about to). Only ever grows; this is what the API reports.
    pub consumed: Duration,
    pub limit: Duration,
    /// Estimated time of currently running project tasks. Counted by the limit checks but
    /// never reported or persisted; replaced by the exact exec time when a task finishes.
    pub in_flight: Duration,
}

impl Project {
    pub fn update_consumed(&mut self, delta: Duration) {
        self.consumed += delta;
    }

    pub fn add_in_flight(&mut self, delta: Duration) {
        self.in_flight += delta;
    }

    /// Settles a finished task: drops its `charged` in-flight estimate and confirms the
    /// `actual` consumed time, in whole milliseconds as the DB stores it (`tasks.exec_time_ms`),
    /// so the in-memory `consumed` matches the DB's `consumed_ms` exactly.
    pub fn settle_in_flight(&mut self, charged: Duration, actual: Duration) {
        self.in_flight = self.in_flight.saturating_sub(charged);
        self.consumed += Duration::from_millis(actual.as_millis() as u64);
    }

    pub fn is_over_limit(&self) -> bool {
        self.consumed + self.in_flight >= self.limit
    }

    pub fn has_time_for(&self, duration: Duration) -> bool {
        self.consumed + self.in_flight + duration < self.limit
    }
}

#[derive(Default)]
pub(crate) struct ProjectMap {
    projects: HashMap<ProjectId, Project>,
    projects_by_name: HashMap<String, ProjectId>,
}

impl ProjectMap {
    pub fn get_project_mut(&mut self, id: ProjectId) -> &mut Project {
        self.projects.get_mut(&id).unwrap()
    }

    pub fn find_project(&self, id: ProjectId) -> Option<&Project> {
        self.projects.get(&id)
    }

    pub fn find_project_id_by_name(&self, name: &str) -> Option<ProjectId> {
        self.projects_by_name.get(name).copied()
    }

    pub fn find_project_by_name(&self, name: &str) -> Option<&Project> {
        let id = self.find_project_id_by_name(name)?;
        self.projects.get(&id)
    }

    pub fn add_project(&mut self, project: Project) {
        self.projects_by_name
            .insert(project.name.clone(), project.id);
        self.projects.insert(project.id, project);
    }
}
