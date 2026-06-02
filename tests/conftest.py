from pytest_httpserver import HTTPServer
import socket

from pytest import fixture
from utils import QScheduler
from utils_iqm import IqmFakeBackend


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@fixture(scope="session")
def qscheduler_port() -> int:
    return _free_port()


@fixture(scope="function")
def threaded_httpserver():
    server = HTTPServer(threaded=True)
    server.start()
    yield server
    server.clear()
    if server.is_running():
        server.stop()


@fixture(scope="function")
def iqm_backend(threaded_httpserver):
    yield IqmFakeBackend(threaded_httpserver)
    threaded_httpserver.check()


@fixture(scope="function")
def qscheduler_iqm(tmp_path, qscheduler_port, iqm_backend):
    qs = QScheduler(str(tmp_path), qscheduler_port, backend=iqm_backend)
    yield qs
    qs.cleanup()


class TestBackend:
    def __init__(self):
        pass

    def start(self):
        pass

    def build_config(self):
        return 'type = "test"'


@fixture(scope="function")
def qscheduler_test(tmp_path, qscheduler_port):
    qs = QScheduler(str(tmp_path), qscheduler_port, backend=TestBackend())
    yield qs
    qs.cleanup()


@fixture(scope="function", params=["qscheduler_iqm", "qscheduler_test"])
def qscheduler(request):
    yield request.getfixturevalue(request.param)
