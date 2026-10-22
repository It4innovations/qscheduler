"""Pulse-level tasks (`kind=pulse`) and the QC-artifact proxy used to compile them."""

import json

from utils import TestTask as TT

# Stand-in for a serialized IQM `RunDefinition`; qscheduler forwards it without parsing.
RUN_DEFINITION = b"\x0a\x04test\x12\x00\xff"
MIB = 1024 * 1024


def test_pulse_rejected_without_pulse_access(qscheduler):
    qscheduler.start()
    r = qscheduler.submit(TT(), kind="pulse", expect_error=422)
    assert "pulse" in r.text


def test_pulse_task_test_backend(qscheduler_test):
    qs = qscheduler_test
    qs.pulse_access = True
    qs.start()
    t = qs.submit(TT().compute_time(0.5), kind="pulse")
    qs.wait_for_finished(t)
    info = qs.get_task(t)
    assert info["kind"] == "pulse"
    assert info["exectime_ms"] > 0
    assert qs.get_task_artifact(t, "kind") == "pulse"


def test_default_kind_is_circuit(qscheduler_test):
    qs = qscheduler_test
    qs.pulse_access = True
    qs.start()
    t = qs.submit(TT())
    qs.wait_for_finished(t)
    assert qs.get_task(t)["kind"] == "circuit"
    assert qs.get_task_artifact(t, "kind") == "circuit"


def test_content_type_must_match_kind(qscheduler_test):
    qs = qscheduler_test
    qs.pulse_access = True
    qs.start()
    qs.submit(TT(), kind="pulse", content_type="application/json", expect_error=422)
    qs.submit(TT(), content_type="application/protobuf", expect_error=422)
    qs.submit(TT(), kind="bogus", expect_error=400)


def test_pulse_task_iqm(qscheduler_iqm, iqm_backend):
    qs = qscheduler_iqm
    qs.pulse_access = True
    qs.start()
    t = qs.submit(
        None,
        kind="pulse",
        payload=RUN_DEFINITION,
        content_type="application/protobuf",
    )
    qs.wait_for_finished(t)
    assert iqm_backend.run_bodies == [RUN_DEFINITION]
    assert qs.get_task(t)["kind"] == "pulse"

    r = qs.get_task_artifact_response(t, "sweep_results")
    assert r.headers["Content-Type"] == "application/protobuf"
    assert r.content == RUN_DEFINITION


def test_circuit_task_iqm_still_uses_circuit_route(qscheduler_iqm, iqm_backend):
    qs = qscheduler_iqm
    qs.pulse_access = True
    qs.start()
    t = qs.submit(TT())
    qs.wait_for_finished(t)
    assert iqm_backend.run_bodies == []
    r = qs.get_task_artifact_response(t, "measurements")
    assert r.headers["Content-Type"] == "application/json"


def test_restore_waiting_pulse_task(qscheduler_test):
    """A pulse task still queued at a crash is re-submitted as pulse."""
    qs = qscheduler_test
    qs.pulse_access = True
    qs.queue_size = 1
    qs.start()
    t1 = qs.submit(TT().compute_time(3))
    t2 = qs.submit(TT(), kind="pulse")
    qs.wait_for_running(t1)
    qs.assert_task_waiting(t2)

    qs.restart()

    qs.wait_for_finished(t2)
    assert qs.get_task(t2)["kind"] == "pulse"
    assert qs.get_task_artifact(t2, "kind") == "pulse"


def test_kind_survives_restart_for_finished_task(qscheduler_test):
    qs = qscheduler_test
    qs.pulse_access = True
    qs.start()
    t = qs.submit(TT(), kind="pulse")
    qs.wait_for_finished(t)
    qs.restart()
    # Terminal tasks are served from the DB after a restart.
    assert qs.get_task(t)["kind"] == "pulse"


def test_machine_artifact_iqm(qscheduler_iqm):
    qs = qscheduler_iqm
    qs.start()
    r = qs.get_machine_artifact("settings", accept="application/protobuf")
    assert r.headers["Content-Type"] == "application/protobuf"
    assert r.content == b"\x0asettings"

    r = qs.get_machine_artifact("chip-design-records")
    assert r.json() == [{"cheddar": {"chip_label": "Q5"}}]

    # Pulla reads the chip label from `duts`.
    r = qs.get_machine_artifact("duts")
    assert r.json() == [{"label": "Q5_chip", "dut_type": "chip"}]

    assert qs.get_machine_calibration_set()["observations"] == []


def test_machine_artifact_test_backend(qscheduler_test):
    qs = qscheduler_test
    qs.start()
    r = qs.get_machine_artifact("settings", accept="application/protobuf")
    assert json.loads(r.text) == {
        "artifact": "settings",
        "accept": "application/protobuf",
    }


def test_machine_artifact_unknown_name(qscheduler):
    qscheduler.start()
    qscheduler.get_machine_artifact("secrets", expect_error=404)
    qscheduler.get_machine_artifact("settings", machine="nope", expect_error=404)


def _padded_payload(size: int) -> bytes:
    """A valid test-backend payload padded with trailing JSON whitespace to `size` bytes."""
    payload = TT().create_payload()
    return payload + b" " * (size - len(payload))


def test_large_payload_default_limit(qscheduler_test):
    """Compiled pulse jobs are several MiB, well over axum's 2 MiB default limit."""
    qs = qscheduler_test
    qs.pulse_access = True
    qs.start()
    t = qs.submit(None, kind="pulse", payload=_padded_payload(5 * MIB))
    qs.wait_for_finished(t)
    qs.submit(None, payload=_padded_payload(33 * MIB), expect_error=413)


def test_payload_limit_configurable(qscheduler_test):
    qs = qscheduler_test
    qs.max_payload_mb = 1
    qs.start()
    t = qs.submit(None, payload=_padded_payload(MIB - 1024))
    qs.wait_for_finished(t)
    qs.submit(None, payload=_padded_payload(MIB + 1024), expect_error=413)
