from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from pytest_httpserver import HTTPServer
import os
import socket
import subprocess
from urllib.parse import urlparse
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from pytest import fixture
from utils import QScheduler
from utils_iqm import IqmFakeBackend


@fixture
def db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.fail("DATABASE_URL environment variable is not set")
    return url


@fixture
def db(db_url) -> str:
    parsed = urlparse(db_url)
    base_name = parsed.path.lstrip("/")
    temp_name = f"{base_name}_test_{uuid4().hex[:8]}"
    temp_url = db_url[: db_url.rfind("/")] + f"/{temp_name}"
    admin_url = db_url[: db_url.rfind("/")] + "/postgres"

    def admin_connect():
        conn = psycopg2.connect(admin_url)
        conn.autocommit = True
        return conn

    def terminate_connections(cur):
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname = %s AND pid <> pg_backend_pid()",
            [temp_name],
        )

    admin_conn = admin_connect()
    try:
        with admin_conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(temp_name)))
    finally:
        admin_conn.close()

    subprocess.run(
        ["sqlx", "migrate", "run"],
        check=True,
        env={**os.environ, "DATABASE_URL": temp_url},
    )

    yield temp_url

    admin_conn = admin_connect()
    try:
        with admin_conn.cursor() as cur:
            terminate_connections(cur)
            cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(temp_name)))
    finally:
        admin_conn.close()


@fixture
def db_connect(db):
    conn = psycopg2.connect(db)
    conn.autocommit = True
    yield conn
    conn.close()


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


class TestBackend:
    def __init__(self):
        pass

    def start(self):
        pass

    def machine_args(self):
        return ["test"]


@fixture(scope="function")
def qscheduler_iqm(tmp_path, qscheduler_port, iqm_backend, db):
    qs = QScheduler(
        str(tmp_path), qscheduler_port, backend=iqm_backend, database_url=db
    )
    yield qs
    qs.cleanup()


@fixture(scope="function")
def qscheduler_test(tmp_path, qscheduler_port, db):
    qs = QScheduler(
        str(tmp_path), qscheduler_port, backend=TestBackend(), database_url=db
    )
    yield qs
    qs.cleanup()


@fixture(scope="function", params=["qscheduler_iqm", "qscheduler_test"])
def qscheduler(request):
    yield request.getfixturevalue(request.param)
