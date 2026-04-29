
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, Default)]
pub struct MachineId(u32);

pub struct Machine {
    machine_id: MachineId,
    name: String,
}

impl From<u32> for MachineId {
    fn from(v: u32) -> Self {
        MachineId(v)
    }
}