def test_version(qscheduler_test):
    qscheduler_test.start()
    assert qscheduler_test.version().startswith("qscheduler v")


def test_health(qscheduler_test):
    qscheduler_test.start()
    r = qscheduler_test.health()
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
