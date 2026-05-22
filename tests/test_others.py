def test_version(qscheduler):
    assert qscheduler.version().startswith("qscheduler v")
