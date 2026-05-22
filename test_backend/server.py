#!/usr/bin/env python3
"""Mock backend server: spawns per-task worker servers on demand using MIME for tasking."""

import logging
import threading
import uuid
import json
import socket
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, File, Form, UploadFile
from fastapi.responses import JSONResponse

from qiskit import QuantumCircuit
from qiskit.qasm3 import loads as qasm3loads

from iqm.qiskit_iqm.fake_backends.fake_deneb import IQMFakeDeneb
from iqm.station_control.interface.models import StaticQuantumArchitecture

logging.getLogger("uvicorn").setLevel(logging.ERROR)

#################################
# Business Logic and Interface  #
#################################


class QTask:
    def __init__(self, circuits: list[QuantumCircuit], execution_kwargs: dict):
        self._circuits = circuits
        self._execution_kwargs = execution_kwargs

    @property
    def circuits(self):
        return self._circuits

    @property
    def execution_kwargs(self):
        return self._execution_kwargs


class IQMWorker:
    def __init__(self):
        self.iqm_backend = IQMFakeDeneb()
        self.architecture = StaticQuantumArchitecture(
            dut_label="M139_W0_N60_Z99",
            qubits=["QB1", "QB2", "QB3", "QB4", "QB5", "QB6"],
            computational_resonators=["CR1"],
            connectivity=[
                ("CR1", "QB1"),
                ("CR1", "QB2"),
                ("CR1", "QB3"),
                ("CR1", "QB4"),
                ("CR1", "QB5"),
                ("CR1", "QB6"),
            ],
        )
        self.metrics = self.iqm_backend.metrics
        self.prepared_circuits: dict[uuid.UUID, QTask] = {}

        # Calibration ID from the fake backend
        self.calibration_id = self.iqm_backend.architecture.calibration_set_id
        print(f"[worker] initialized for calibration id {self.calibration_id}")

    def set_task(
        self, calibration_id: uuid.UUID, execution_args: dict, qasm_content: str
    ):
        """Worker logic for preparing a task, now receiving clean types."""
        if calibration_id != self.calibration_id:
            print(
                f"[worker] Calibration mismatch. Requested: {calibration_id}, Available: {self.calibration_id}"
            )
            return {"error_msg": "Unsupported calibration identifier!"}, 400

        task_id = uuid.uuid4()
        while task_id in self.prepared_circuits:
            task_id = uuid.uuid4()

        try:
            circuit = qasm3loads(qasm_content)
            self.prepared_circuits[task_id] = QTask([circuit], execution_args)
        except Exception as e:
            return {"error_msg": f"Failed to load OpenQASM: {str(e)}"}, 400

        print(f"[worker] task '{str(task_id)}' assigned and compiled")
        return {"qtask_id": str(task_id)}, 200

    def start_task(self, task_uuid: uuid.UUID):
        qtask = self.prepared_circuits.get(task_uuid)
        if qtask is None:
            return {"error_msg": f"Task {str(task_uuid)} not found!"}, 404

        print(f"[worker] task '{str(task_uuid)}' started")
        qjob = self.iqm_backend.run(qtask.circuits, **qtask.execution_kwargs)
        qresult = qjob.result()

        if not qresult.success:
            return {"error_msg": "Execution Failed!"}, 400

        return qresult.to_dict(), 200

    def delete_task(self, task_uuid: uuid.UUID):
        if task_uuid not in self.prepared_circuits:
            return {"error_msg": f"Task {str(task_uuid)} not found!"}, 404

        del self.prepared_circuits[task_uuid]
        return {"qtask_id": str(task_uuid), "status": "deleted"}, 200


def make_worker_app(worker: IQMWorker) -> FastAPI:
    app = FastAPI()

    @app.get("/architecture")
    def get_quantum_architecture():
        return {
            "calibration_id": worker.calibration_id,
            "architecture": worker.architecture,
        }

    @app.get("/metrics")
    def get_metrics():
        return {"calibration_id": worker.calibration_id, "metrics": worker.metrics}

    @app.post("/task")
    async def accept_task(
        calibration_id: str = Form(...),
        execution_args: str = Form(...),
        openqasm: UploadFile = File(...),
    ):
        """MIME-based task submission."""
        try:
            # Parse multipart fields
            cal_uuid = uuid.UUID(calibration_id)
            exec_dict = json.loads(execution_args)
            qasm_bytes = await openqasm.read()
            qasm_str = qasm_bytes.decode("utf-8")
        except Exception as e:
            return JSONResponse(
                {"error_msg": f"MIME parsing failed: {str(e)}"}, status_code=400
            )

        content, status = worker.set_task(cal_uuid, exec_dict, qasm_str)
        return JSONResponse(content=content, status_code=status)

    @app.post("/task/start")
    async def start_task(request: Request):
        # We'll keep the start/delete body as raw bytes for simplicity,
        # or you can change them to path parameters.
        try:
            task_uuid = uuid.UUID(bytes=await request.body())
            content, status = worker.start_task(task_uuid)
            return JSONResponse(content=content, status_code=status)
        except ValueError:
            return JSONResponse({"error_msg": "Invalid UUID bytes"}, status_code=400)

    @app.delete("/task")
    async def delete_task(request: Request):
        try:
            task_uuid = uuid.UUID(bytes=await request.body())
            content, status = worker.delete_task(task_uuid)
            return JSONResponse(content=content, status_code=status)
        except ValueError:
            return JSONResponse({"error_msg": "Invalid UUID bytes"}, status_code=400)

    return app


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


main_app = FastAPI(lifespan=lifespan)


@main_app.post("/worker")
def new_worker():
    worker = IQMWorker()
    app = make_worker_app(worker)

    # Find an available port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = uvicorn.Config(app, host="localhost", port=port, log_level="error")
    server = uvicorn.Server(config)

    threading.Thread(target=server.run, daemon=True).start()
    url = f"http://localhost:{port}"
    print(f"[main] worker started at {url}")
    return {"url": url}


if __name__ == "__main__":
    print("Main Gateway listening on http://localhost:8080")
    uvicorn.run(main_app, host="localhost", port=8080, log_level="error")
