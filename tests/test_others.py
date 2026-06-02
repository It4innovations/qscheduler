def test_version(qscheduler):
    qscheduler.start()
    assert qscheduler.version().startswith("qscheduler v")
