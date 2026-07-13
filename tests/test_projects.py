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
    qscheduler_test.queue_size = 1
    qscheduler_test.start(add_test_project=False)
    qscheduler_test.add_project("myproject", limit_ms=100)

    task1 = qscheduler_test.submit(TT().compute_time(0.2), project="myproject")
    task2 = qscheduler_test.submit(TT(), project="myproject")

    qscheduler_test.wait_for_finished(task1)
    qscheduler_test.wait_for_failed(task2)
    assert "time limit" in qscheduler_test.get_task(task2)["error"].lower()
