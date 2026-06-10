def test_version(qscheduler_test):
    qscheduler_test.start()
    assert qscheduler_test.version().startswith("qscheduler v")
