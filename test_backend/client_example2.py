import time
import requests
from qiskit import QuantumCircuit, transpile
from qiskit.qasm3 import dumps as qasm3dumps
from iqm.qiskit_iqm.fake_backends.fake_deneb import IQMFakeDeneb

QSCHEDULER = "http://localhost:3000"

# 1. Build circuit
qc = QuantumCircuit(2, 2)
qc.h(0)
qc.cx(0, 1)
qc.measure_all()

# 2. Transpile for IQM Deneb architecture (same target qscheduler workers use)
backend = IQMFakeDeneb()
qc_transpiled = transpile(qc, backend=backend)

# 3. Serialize to OpenQASM 3 (patch missing 'move' gate definition before export)
for instr in qc_transpiled.data:
    if instr.operation.name == "move" and not getattr(instr.operation, "definition", None):
        dummy = QuantumCircuit(instr.operation.num_qubits)
        instr.operation.definition = dummy
qasm_bytes = qasm3dumps(qc_transpiled).encode("utf-8")

# 4. Submit to qscheduler — metadata as query params, circuit as raw body
params = {
    "machine_id": 0,
    "repeats": 1,
    "max_waiting_time_secs": 60,
    "max_compute_time_secs": 120,
}
resp = requests.post(
    f"{QSCHEDULER}/tasks",
    params=params,
    data=qasm_bytes,
    headers={"Content-Type": "application/octet-stream"},
)
resp.raise_for_status()
task_id = resp.json()
print(f"Task submitted: id={task_id}")

# 5. Poll until finished or failed
while True:
    state_resp = requests.get(f"{QSCHEDULER}/tasks/{task_id}")
    state_resp.raise_for_status()
    state = state_resp.json()
    print(f"Task {task_id} state: {state}")
    if state in ("finished", "failed"):
        break
    time.sleep(1)
