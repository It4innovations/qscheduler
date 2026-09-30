import time

from utils import TEST_PROJECT
from utils import TestTask as TT


def test_create_and_list_projects(qscheduler_test):
    qscheduler_test.start(add_test_project=False)

    qscheduler_test.add_project("alpha", limit_ms=10_000)
    qscheduler_test.add_project("beta", limit_ms=20_000, active=False)
    qscheduler_test.add_project("gamma", limit_ms=30_000)

    projects = qscheduler_test.list_projects()

    assert len(projects) == 3
    by_id = {p["name"]: p for p in projects}

    assert by_id["alpha"] == {
        "name": "alpha",
        "consumed_ms": 0,
        "limit_ms": 10_000,
        "active": True,
    }
    assert by_id["beta"] == {
        "name": "beta",
        "consumed_ms": 0,
        "limit_ms": 20_000,
        "active": False,
    }
    assert by_id["gamma"] == {
        "name": "gamma",
        "consumed_ms": 0,
        "limit_ms": 30_000,
        "active": True,
    }


def test_update_project_active_and_limit(qscheduler_test):
    qscheduler_test.start(add_test_project=False)
    qscheduler_test.add_project("alpha", limit_ms=10_000)

    qscheduler_test.update_project("alpha", active=False)
    project = qscheduler_test.get_project("alpha")
    assert project["active"] is False
    assert project["limit_ms"] == 10_000

    qscheduler_test.update_project("alpha", limit_ms=5_000)
    project = qscheduler_test.get_project("alpha")
    assert project["active"] is False
    assert project["limit_ms"] == 5_000

    qscheduler_test.update_project("alpha", active=True, limit_ms=20_000)
    project = qscheduler_test.get_project("alpha")
    assert project["active"] is True
    assert project["limit_ms"] == 20_000


def test_update_project_persisted_after_restart(qscheduler_test):
    qscheduler_test.start(add_test_project=False)
    qscheduler_test.add_project("alpha", limit_ms=10_000)

    qscheduler_test.update_project("alpha", active=False, limit_ms=5_000)
    qscheduler_test.restart()

    project = qscheduler_test.get_project("alpha")
    assert project["active"] is False
    assert project["limit_ms"] == 5_000


def test_update_project_not_found(qscheduler_test):
    qscheduler_test.start(add_test_project=False)
    qscheduler_test.update_project("nonexistent", active=False, expect_status_code=404)


def test_update_project_reactivation_allows_submit(qscheduler):
    qscheduler.start(add_test_project=False)
    qscheduler.add_project("myproject", limit_ms=10_000, active=False)
    qscheduler.submit(TT(), project="myproject", expect_error=402)

    qscheduler.update_project("myproject", active=True)
    task_id = qscheduler.submit(TT(), project="myproject")
    qscheduler.wait_for_finished(task_id)


def test_duplicate_name_fail(qscheduler_test):
    qscheduler_test.start(add_test_project=False)
    qscheduler_test.add_project("alpha", limit_ms=10_000)
    qscheduler_test.add_project("alpha", limit_ms=20_000, expect_status_code=409)


def test_consumed_time_single_task(qscheduler):
    qscheduler.start()

    project = qscheduler.get_project("test-project")
    assert project["consumed_ms"] == 0

    task_id = qscheduler.submit(TT().compute_time(0.2))
    qscheduler.wait_for_finished(task_id)

    project = qscheduler.get_project("test-project")
    assert 100 < project["consumed_ms"] < 300


def test_consumed_time_single_task_after_restart(qscheduler):
    qscheduler.start()
    qscheduler.restart()

    project = qscheduler.get_project("test-project")
    assert project["consumed_ms"] == 0

    task_id = qscheduler.submit(TT().compute_time(0.2))
    qscheduler.wait_for_finished(task_id)

    project = qscheduler.get_project("test-project")
    assert 100 < project["consumed_ms"] < 300


def test_consumed_time_many_tasks(qscheduler):
    qscheduler.start(add_test_project=False)
    projects = ["A", "B", "C"]
    for p in projects:
        qscheduler.add_project(p, 10_000)

    tasks = []
    for i in range(5):
        for x, p in enumerate(projects):
            tasks.append(qscheduler.submit(TT().compute_time(0.1 * (x + 1)), project=p))
    for t in reversed(tasks):
        qscheduler.wait_for_finished(t)

    for x, p in enumerate(projects):
        project = qscheduler.get_project(p)
        expected = (x + 1) * 500
        assert expected - 100 < project["consumed_ms"] < expected + 100


def test_submit_rejected_when_quota_exceeded(qscheduler):
    # Keep the periodic in-flight project check from failing the task while it runs.
    qscheduler.limit_check_interval = 60
    qscheduler.start(add_test_project=False)
    qscheduler.add_project("myproject", limit_ms=1)

    task_id = qscheduler.submit(TT().compute_time(0.1), project="myproject")
    qscheduler.wait_for_finished(task_id)

    qscheduler.submit(TT(), project="myproject", expect_error=402)


def test_task_submit_rejected_when_project_inactive(qscheduler):
    qscheduler.start(add_test_project=False)
    qscheduler.add_project("myproject", limit_ms=10_000, active=False)

    qscheduler.submit(TT(), project="myproject", expect_error=402)


def test_session_create_rejected_when_project_inactive(qscheduler):
    qscheduler.start(add_test_project=False)
    qscheduler.add_project("myproject", limit_ms=10_000, active=False)

    qscheduler.new_session(time_limit=1, project="myproject", expect_error=402)


def test_task_failed_when_quota_exceeded_in_launcher(qscheduler_test):
    # Keep the periodic in-flight project check from failing task1 while it runs.
    qscheduler_test.limit_check_interval = 60
    qscheduler_test.queue_size = 1
    qscheduler_test.start(add_test_project=False)
    qscheduler_test.add_project("myproject", limit_ms=100)

    task1 = qscheduler_test.submit(TT().compute_time(0.2), project="myproject")
    task2 = qscheduler_test.submit(TT(), project="myproject")

    qscheduler_test.wait_for_finished(task1)
    qscheduler_test.wait_for_failed(task2)
    assert "time limit" in qscheduler_test.get_task(task2)["error"].lower()


def test_running_tasks_failed_when_project_limit_exceeded(qscheduler):
    qscheduler.limit_check_interval = 1
    qscheduler.queue_size = 2
    qscheduler.start(add_test_project=False)
    qscheduler.add_project("myproject", limit_ms=1_500)

    t1 = qscheduler.submit(TT().compute_time(30), project="myproject")
    t2 = qscheduler.submit(TT().compute_time(30), project="myproject")
    for t in (t1, t2):
        qscheduler.wait_for_failed(t)
        assert qscheduler.get_task(t)["error"] == "Project time limit exceeded"

    # In-flight charges were replaced by the backend's exact exec times.
    exec_ms = sum(qscheduler.get_task(t).get("exectime_ms", 0) for t in (t1, t2))
    consumed = qscheduler.get_project("myproject")["consumed_ms"]
    assert consumed >= 1_500
    assert abs(consumed - exec_ms) < 100

    qscheduler.submit(TT(), project="myproject", expect_error=402)


def test_queued_tasks_failed_after_running_task_exhausts_project(qscheduler_test):
    qscheduler_test.limit_check_interval = 1
    qscheduler_test.queue_size = 1
    qscheduler_test.start(add_test_project=False)
    qscheduler_test.add_project("myproject", limit_ms=1_500)

    t1 = qscheduler_test.submit(TT().compute_time(30), project="myproject")
    t2 = qscheduler_test.submit(TT().compute_time(30), project="myproject")
    t3 = qscheduler_test.submit(TT(), project="myproject")
    for t in (t1, t2, t3):
        qscheduler_test.wait_for_failed(t)
        assert qscheduler_test.get_task(t)["error"] == "Project time limit exceeded"
    # t2/t3 were never submitted to the backend.
    assert "started_at" not in qscheduler_test.get_task(t2)
    assert "started_at" not in qscheduler_test.get_task(t3)


def _listed_consumed(qs, name):
    return next(p for p in qs.list_projects() if p["name"] == name)["consumed_ms"]


def _wait_listed_consumed(qs, name, expected):
    """GET /projects reads the DB, whose write lands just after the in-memory update."""
    deadline = time.monotonic() + 2
    while (
        value := _listed_consumed(qs, name)
    ) != expected and time.monotonic() < deadline:
        time.sleep(0.05)
    return value


def test_project_consumption_excludes_running_tasks(qscheduler_test):
    """Reported consumption is confirmed time only; running tasks' estimates count toward
    the limit checks but never show up in the API."""
    qs = qscheduler_test
    qs.limit_check_interval = 1
    qs.start()

    task_id = qs.submit(TT().compute_time(30))
    qs.wait_for_running(task_id)
    time.sleep(2.5)

    assert _listed_consumed(qs, TEST_PROJECT) == 0
    assert qs.get_project(TEST_PROJECT)["consumed_ms"] == 0

    qs.cancel_task(task_id)
    qs.wait_for_cancelled(task_id)
    exec_ms = qs.get_task(task_id)["exectime_ms"]
    assert qs.get_project(TEST_PROJECT)["consumed_ms"] == exec_ms
    assert _wait_listed_consumed(qs, TEST_PROJECT, exec_ms) == exec_ms


def test_project_consumption_never_decreases(qscheduler):
    """While running tasks are charged, cancelled for the project limit, and settled with their
    exact exec times, reported consumption only ever grows."""
    qs = qscheduler
    qs.limit_check_interval = 1
    qs.queue_size = 2
    qs.start(add_test_project=False)
    qs.add_project("myproject", limit_ms=1_500)

    tasks = [qs.submit(TT().compute_time(30), project="myproject") for _ in range(2)]
    # Sampled per endpoint: the list (DB) may briefly lag the single-project endpoint
    # (memory) by the DB write, but neither may ever go backwards.
    single, listed = [], []
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        single.append(qs.get_project("myproject")["consumed_ms"])
        listed.append(_listed_consumed(qs, "myproject"))
        if all(qs.get_task_state(t) == "failed" for t in tasks):
            break
        time.sleep(0.1)
    assert all(qs.get_task_state(t) == "failed" for t in tasks)

    # Settled with the exact whole-ms exec times, so memory and DB agree exactly.
    exec_ms = sum(qs.get_task(t).get("exectime_ms", 0) for t in tasks)
    single.append(qs.get_project("myproject")["consumed_ms"])
    listed.append(_wait_listed_consumed(qs, "myproject", exec_ms))
    assert single == sorted(single)
    assert listed == sorted(listed)
    assert single[-1] == listed[-1] == exec_ms
    assert exec_ms >= 1_500
