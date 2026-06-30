use crate::SessionId;
use std::collections::HashMap;
use std::fmt::Display;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct ProjectId(NonZeroU32);

impl Display for ProjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<u32> for ProjectId {
    type Error = ();
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        NonZeroU32::new(v).map(ProjectId).ok_or(())
    }
}

impl TryFrom<i32> for ProjectId {
    type Error = ();
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        let v = u32::try_from(v).map_err(|_| ())?;
        NonZeroU32::new(v).map(ProjectId).ok_or(())
    }
}

impl ProjectId {
    pub fn as_i32(self) -> i32 {
        self.0.get() as i32
    }
}

#[derive(Debug, Clone)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub active: bool,
    pub consumed: Duration,
    pub limit: Duration,
}

impl Project {
    pub fn update_consumed(&mut self, delta: Duration) {
        self.consumed += delta;
    }

    pub fn is_over_limit(&self) -> bool {
        self.consumed >= self.limit
    }

    pub fn has_time_for(&self, duration: Duration) -> bool {
        self.consumed + duration < self.limit
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
