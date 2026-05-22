import socket

from pytest import fixture
from utils import QScheduler


@fixture(scope="session")
def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@fixture(scope="function")
def qscheduler(tmp_path, free_port):
    qs = QScheduler(str(tmp_path), free_port)
    qs.start()
    yield qs
    qs.cleanup()
