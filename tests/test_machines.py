from utils_iqm import ABOUT


def test_machine_about_test_backend(qscheduler_test):
    qscheduler_test.start()
    assert qscheduler_test.get_machine_about() == {"type": "test", "version": "1.0"}


def test_machine_about_iqm(qscheduler_iqm):
    qscheduler_iqm.start()
    assert qscheduler_iqm.get_machine_about() == {
        "type": "iqm",
        "version": {
            "qccsw_version": ABOUT["qccsw_version"],
            "server_version": ABOUT["server_version"],
            "station_control_version": ABOUT["station_control_version"],
        },
    }


def test_machine_about_unknown_machine(qscheduler):
    qscheduler.start()
    qscheduler.get_machine_about("no-such-machine", expect_error=404)


def test_machine_about_iqm_backend_error(qscheduler_iqm, threaded_httpserver):
    qscheduler_iqm.start()
    # Handlers are matched in registration order, so drop the fake's /about handler
    # and replace it with a failing one.
    threaded_httpserver.clear_all_handlers()
    threaded_httpserver.expect_request("/about").respond_with_data("boom", status=503)
    qscheduler_iqm.get_machine_about(expect_error=500)
