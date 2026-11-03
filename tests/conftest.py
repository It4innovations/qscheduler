import os
import random
import socket
import subprocess
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import psycopg2
import pytest
from dotenv import load_dotenv
from psycopg2 import sql
from pytest import fixture
from pytest_httpserver import HTTPServer
from utils import QScheduler
from utils_iqm import IqmFakeBackend

load_dotenv(Path(__file__).parent.parent / ".env")


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
    """A port below Linux's default ephemeral range (32768+): nothing holds it between a restart
    of the service, so a client connection of a parallel test must not get it assigned."""
    while True:
        port = random.randrange(10_000, 32_000)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port


@fixture
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
