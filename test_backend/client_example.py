import requests
import uuid
import json

# Use for production
# from iqm.qiskit_iqm.iqm_backend import IQMBackendBase
from iqm.station_control.interface.models import StaticQuantumArchitecture
from iqm.qiskit_iqm.fake_backends.fake_deneb import IQMFakeDeneb
from iqm.qiskit_iqm.fake_backends.iqm_fake_backend import (
    IQMFakeBackend,
    IQMErrorProfile,
)
from qiskit import QuantumCircuit
from qiskit.providers import Options, JobV1 as Job
from qiskit.qasm3 import dumps as qasm3dumps
from qiskit.result import Result


class RemoteWorkerJob(Job):
    """A minimal job wrapper to satisfy the Qiskit interface."""

    def __init__(self, backend, result_dict):
        super().__init__(backend, str(uuid.uuid4()))
        self._result = Result.from_dict(result_dict)

    def submit(self):
        pass

    def result(self):
        return self._result

    def status(self):
        return "COMPLETED"


class RemoteWorkerBackend(IQMFakeBackend):
    def __init__(self, url: str):
        self.url = url.rstrip("/")

        # 1. Fetch architecture/metrics from worker
        config = requests.get(f"{self.url}/architecture").json()
        metrics_resp = requests.get(f"{self.url}/metrics").json()

        self.calibration_id = config.get("calibration_id")

        # 2. Initialize the Base
        super().__init__(
            architecture=StaticQuantumArchitecture.model_validate(
                config["architecture"]
            ),
            metrics=metrics_resp["metrics"],
            name="RemoteWorkerBackend",
            error_profile=IQMFakeDeneb().error_profile,
        )

    @property
    def error_profile(self) -> IQMErrorProfile:
        # Fixed recursive call: return the private attribute from superclass
        return self.error_profile

    @classmethod
    def _default_options(cls) -> Options:
        return Options(shots=1024)

    @property
    def max_circuits(self) -> int:
        return 1

    def run(self, run_input, **options) -> Job:
        """Sends task via multipart/form-data (MIME)."""

        # 1. OpenQASM 'move' instruction fix
        if isinstance(run_input, QuantumCircuit):
            for instr in run_input.data:
                if instr.operation.name == "move":
                    if (
                        not hasattr(instr.operation, "definition")
                        or instr.operation.definition is None
                    ):
                        dummy_circ = QuantumCircuit(instr.operation.num_qubits)
                        instr.operation.definition = dummy_circ

        # 2. Serialize circuit to OpenQASM 3 string
        qasm_str = qasm3dumps(run_input)

        # 3. Prepare MIME payload for requests
        # Format: (filename, content, content_type)
        files = {
            "calibration_id": (None, str(self.calibration_id)),
            "execution_args": (None, json.dumps(options)),
            "openqasm": ("circuit.qasm", qasm_str, "text/plain"),
        }

        # 4. Register Task
        resp = requests.post(f"{self.url}/task", files=files)
        resp.raise_for_status()

        # 5. Start Task (using UUID bytes as previously established)
        task_id = resp.json()["qtask_id"]
        task_uuid_bytes = uuid.UUID(task_id).bytes

        exec_resp = requests.post(f"{self.url}/task/start", data=task_uuid_bytes)
        exec_resp.raise_for_status()

        return RemoteWorkerJob(self, exec_resp.json())


if __name__ == "__main__":
    from qiskit import QuantumCircuit, transpile

    # 1. Get a worker instance from the gateway
    GATEWAY = "http://localhost:8080"
    worker_url = requests.post(f"{GATEWAY}/worker").json()["url"]
    print(f"Connected to dynamic worker at: {worker_url}")

    backend = RemoteWorkerBackend(worker_url)

    # 2. Define circuit
    qc = QuantumCircuit(2, 2)
    qc.h(0)  # Changed to H to see something interesting in results
    qc.cx(0, 1)
    qc.measure_all()

    # 3. Transpile explicitly for THIS backend
    qc_transpiled = transpile(qc, backend=backend)

    # 4. Run and Result
    job = backend.run(qc_transpiled, shots=1000)
    result = job.result()
    print(f"Counts: {result.get_counts()}")
