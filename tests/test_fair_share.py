"""Fair sharing of a machine between users, observed through the order tasks start in.

Usage isn't exposed by the API, so a test builds up a difference between users and then checks who
goes next. To make that deterministic, the competing tasks are queued while `machine_held` keeps
the machine busy, and it is released only once all of them are queued. Task IDs grow with
submission, and equal usage is resolved in favor of the lowest ID, so most tests submit the task
expected to go *second* first: it would win if the usage difference was lost.
"""

import time
from contextlib import contextmanager

import pytest
from utils import TEST_MACHINE_NAME, QScheduler
from utils import TestTask as TT

HOLDER = "holder"
# Far longer than any test, so such a task only ends when the test ends it.
LONG = 60


def _quick():
    return TT().compute_time(0.05)


def _run(qs, user, seconds, **kwargs):
    """Runs a task of `user` taking `seconds` to completion, adding it to the user's usage."""
    task = qs.submit(TT().compute_time(seconds), user=user, **kwargs)
    qs.wait_for_finished(task)
    return task


def _long_running(qs, user, **kwargs):
    """Starts a task of `user` that runs until the test ends it."""
    task = qs.submit(TT().compute_time(LONG), user=user, **kwargs)
    qs.wait_for_running(task)
    return task


def _end(qs, task):
    qs.cancel_task(task)
    qs.wait_for_cancelled(task)


@contextmanager
def machine_held(qs, machine=TEST_MACHINE_NAME):
    """Keeps all slots of a machine busy, then frees them on exit."""
    holds = [
        _long_running(qs, HOLDER, machine=machine) for _ in range(qs.cfg.queue_size)
    ]
    yield
    for hold in holds:
        _end(qs, hold)


def _started_at(qs, task):
    return qs.get_task(task)["started_at"]


def assert_start_order(qs, *tasks):
    """Waits for `tasks` to finish, and checks they started in the given order."""
    for task in tasks:
        qs.wait_for_finished(task)
    assert sorted(tasks, key=lambda t: _started_at(qs, t)) == list(tasks)


def _alice_goes_before_bob(qs, **kwargs):
    """Checks that Alice is next. Her task is submitted last, so equal usage favors Bob."""
    with machine_held(qs):
        bob = qs.submit(_quick(), user="bob", **kwargs)
        alice = qs.submit(_quick(), user="alice", **kwargs)
    assert_start_order(qs, alice, bob)


def _bob_goes_before_alice(qs, **kwargs):
    """Checks that Bob is next. His task is submitted last, so equal usage favors Alice."""
    with machine_held(qs):
        alice = qs.submit(_quick(), user="alice", **kwargs)
        bob = qs.submit(_quick(), user="bob", **kwargs)
    assert_start_order(qs, bob, alice)


def _assert_next_is_dave(qs, free_slot, alice_waiting, dave_waiting):
    """Frees `free_slot` and checks Dave's task takes it, although Alice's is older."""
    _end(qs, free_slot)
    qs.wait_for_running(dave_waiting)
    assert qs.get_task_state(alice_waiting) == "waiting"


def _start(qs, queue_size=1, **options):
    qs.config(queue_size=queue_size, **options)
    qs.start()


# Ordering


def test_user_with_less_usage_goes_first(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    _bob_goes_before_alice(qs)


def test_order_follows_usage_of_several_users(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "heavy", 0.9)
    _run(qs, "medium", 0.5)
    _run(qs, "light", 0.1)
    with machine_held(qs):
        heavy = qs.submit(_quick(), user="heavy")
        medium = qs.submit(_quick(), user="medium")
        light = qs.submit(_quick(), user="light")
        new = qs.submit(_quick(), user="new")
    assert_start_order(qs, new, light, medium, heavy)


@pytest.mark.parametrize("first", ["alice", "bob"])
def test_equal_usage_goes_in_submission_order(qscheduler_test, first):
    qs = qscheduler_test
    _start(qs)
    second = "bob" if first == "alice" else "alice"
    with machine_held(qs):
        t1 = qs.submit(_quick(), user=first)
        t2 = qs.submit(_quick(), user=second)
    assert_start_order(qs, t1, t2)


def test_tasks_of_a_user_keep_their_order(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    with machine_held(qs):
        tasks = [qs.submit(_quick(), user="alice") for _ in range(5)]
    assert_start_order(qs, *tasks)


def test_tasks_of_a_user_keep_their_order_among_others(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    with machine_held(qs):
        alice = [qs.submit(_quick(), user="alice") for _ in range(3)]
        bob = [qs.submit(_quick(), user="bob") for _ in range(3)]
    assert_start_order(qs, *alice)
    assert_start_order(qs, *bob)


@pytest.mark.parametrize("users", [("alice", "bob"), ("alice", "bob", "carol")])
def test_users_with_equal_tasks_take_turns(qscheduler_test, users):
    qs = qscheduler_test
    _start(qs)
    per_user = 4
    tasks = {}
    with machine_held(qs):
        # Without fair share, all tasks of the first user would go first.
        for user in users:
            for _ in range(per_user):
                tasks[qs.submit(TT().compute_time(0.1), user=user)] = user
    for task in tasks:
        qs.wait_for_finished(task)

    order = sorted(tasks, key=lambda t: _started_at(qs, t))
    started = dict.fromkeys(users, 0)
    for task in order:
        started[tasks[task]] += 1
        # Task times vary slightly, so users may swap places within a round.
        assert max(started.values()) - min(started.values()) <= 2, [
            tasks[t] for t in order
        ]


def test_user_with_many_tasks_does_not_block_others(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    with machine_held(qs):
        alice = [qs.submit(TT().compute_time(0.2), user="alice") for _ in range(4)]
        bob = qs.submit(_quick(), user="bob")
    qs.wait_for_finished(bob)
    # Only Alice's first task may start before Bob's: her usage is above his zero afterwards.
    assert _started_at(qs, alice[0]) < _started_at(qs, bob)
    qs.wait_for_finished(alice[1])
    assert _started_at(qs, bob) < _started_at(qs, alice[1])
    assert_start_order(qs, *alice)


def test_heavy_user_is_not_starved(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    with machine_held(qs):
        alice = qs.submit(_quick(), user="alice")
        bob = [qs.submit(TT().compute_time(0.2), user="bob") for _ in range(6)]
    qs.wait_for_finished(alice)
    qs.wait_for_finished(bob[-1])
    # Bob goes first, but only until his usage catches up with Alice's.
    assert _started_at(qs, bob[0]) < _started_at(qs, alice) < _started_at(qs, bob[-1])


def test_cancelled_queued_task_is_skipped(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    with machine_held(qs):
        cancelled = qs.submit(_quick(), user="alice")
        alice = qs.submit(_quick(), user="alice")
        bob = qs.submit(_quick(), user="bob")
        _end(qs, cancelled)
    assert_start_order(qs, bob, alice)
    # It never reached the backend, so it doesn't count as a task in the backend either.
    assert "started_at" not in qs.get_task(cancelled)


def test_usage_is_per_machine(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    qs.api_add_machine("second", queue_size=1)
    _run(qs, "alice", 0.5)
    # Her usage counts on the first machine only: on the second one, she wins on the task ID.
    with machine_held(qs, machine="second"):
        alice = qs.submit(_quick(), user="alice", machine="second")
        bob = qs.submit(_quick(), user="bob", machine="second")
    assert_start_order(qs, alice, bob)
    _bob_goes_before_alice(qs)


# Dispatching: fair share state must never stop a machine or take the service down


def test_sequential_tasks_of_single_user_keep_dispatching(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    for _ in range(5):
        _run(qs, "alice", 0.01)


def test_queued_tasks_of_single_user_all_finish(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2)
    tasks = [qs.submit(_quick(), user="alice") for _ in range(8)]
    for task in tasks:
        qs.wait_for_finished(task)


def test_session_opens_after_tasks_finished(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.01)
    session = qs.new_session(time_limit=5, owner="bob")
    qs.wait_for_session_open(session)


def test_user_returning_after_usage_decayed_away(qscheduler_test):
    qs = qscheduler_test
    _start(qs, decay_half_time="10ms")
    _run(qs, "alice", 0.05)
    # Long enough for her entry to be dropped on the next update.
    time.sleep(0.5)
    _run(qs, "bob", 0.05)
    _run(qs, "alice", 0.05)
    _run(qs, "alice", 0.05)


def test_user_whose_only_task_is_running_when_others_finish(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2, decay_half_time="10ms")
    alice = qs.submit(TT().compute_time(0.5), user="alice")
    qs.wait_for_running(alice)
    # Decays while Alice has no usage or queued work, only a running task.
    for _ in range(3):
        _run(qs, "bob", 0.02)
    qs.wait_for_finished(alice)
    _run(qs, "alice", 0.01)


# Tasks in the backend: each counts as usage, priced at the average task time of its user


def test_running_task_counts_as_usage(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2)
    free_slot = _long_running(qs, HOLDER)
    _long_running(qs, "alice")
    alice = qs.submit(_quick(), user="alice")
    dave = qs.submit(TT().compute_time(LONG), user="dave")
    _assert_next_is_dave(qs, free_slot, alice, dave)


def _start_erin_with_cheaper_tasks_in_backend(qs):
    """Leaves Erin with more usage than Alice but a lower average, and both with two running
    tasks: 0.4 + 2 * 0.1 for Erin against 0.3 + 2 * 0.3 for Alice. Returns a slot to free."""
    _start(qs, queue_size=5)
    for _ in range(4):
        _run(qs, "erin", 0.1)
    _run(qs, "alice", 0.3)
    free_slot = _long_running(qs, HOLDER)
    for user in ("alice", "erin"):
        for _ in range(2):
            _long_running(qs, user)
    return free_slot


def _assert_next_is_erin(qs, free_slot, alice_waiting, erin_waiting):
    """Frees `free_slot` and checks Erin's task takes it, although Alice's is older."""
    _end(qs, free_slot)
    qs.wait_for_running(erin_waiting)
    assert qs.get_task_state(alice_waiting) == "waiting"


def test_tasks_in_backend_are_priced_by_average_of_user(qscheduler_test):
    qs = qscheduler_test
    free_slot = _start_erin_with_cheaper_tasks_in_backend(qs)
    alice = qs.submit(_quick(), user="alice")
    erin = qs.submit(TT().compute_time(LONG), user="erin")
    _assert_next_is_erin(qs, free_slot, alice, erin)


def test_reconfiguration_keeps_average_of_user(qscheduler_test):
    qs = qscheduler_test
    free_slot = _start_erin_with_cheaper_tasks_in_backend(qs)
    alice = qs.submit(_quick(), user="alice")
    erin = qs.submit(TT().compute_time(LONG), user="erin")
    qs.api_update_machine(queue_size=5)
    _assert_next_is_erin(qs, free_slot, alice, erin)


def test_task_of_user_without_average_is_priced_by_placeholder(qscheduler_test):
    """Dave has no usage at all, but his running task is priced far above Alice's usage."""
    qs = qscheduler_test
    _start(qs, queue_size=2)
    _run(qs, "alice", 0.5)
    free_slot = _long_running(qs, HOLDER)
    _long_running(qs, "dave")
    dave = qs.submit(_quick(), user="dave")
    alice = qs.submit(TT().compute_time(LONG), user="alice")
    _end(qs, free_slot)
    qs.wait_for_running(alice)
    assert qs.get_task_state(dave) == "waiting"


def test_task_without_execution_time_does_not_set_average(qscheduler_test):
    """An average of zero would make Alice's running task free, letting her go before Bob."""
    qs = qscheduler_test
    _start(qs, queue_size=2)
    _finish_cancel_before_start(qs)
    _run(qs, "bob", 0.2)
    free_slot = _long_running(qs, HOLDER)
    _long_running(qs, "alice")
    alice = qs.submit(_quick(), user="alice")
    bob = qs.submit(TT().compute_time(LONG), user="bob")
    _end(qs, free_slot)
    qs.wait_for_running(bob)
    assert qs.get_task_state(alice) == "waiting"


def test_tasks_in_backend_add_up(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=4)
    free_slot = _long_running(qs, HOLDER)
    _long_running(qs, "alice")
    _long_running(qs, "alice")
    _long_running(qs, "erin")
    alice = qs.submit(_quick(), user="alice")
    erin = qs.submit(TT().compute_time(LONG), user="erin")
    _end(qs, free_slot)
    qs.wait_for_running(erin)
    assert qs.get_task_state(alice) == "waiting"


def test_dispatched_task_counts_for_next_pick(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2)
    holds = [_long_running(qs, HOLDER) for _ in range(2)]
    a1 = qs.submit(TT().compute_time(LONG), user="alice")
    a2 = qs.submit(TT().compute_time(LONG), user="alice")
    b1 = qs.submit(TT().compute_time(LONG), user="bob")
    _end(qs, holds[0])
    qs.wait_for_running(a1)
    # Neither user has any usage yet, only Alice's first task on the backend.
    _end(qs, holds[1])
    qs.wait_for_running(b1)
    assert qs.get_task_state(a2) == "waiting"


def _finish_ok(qs):
    task = qs.submit(_quick(), user="alice")
    qs.wait_for_finished(task)


def _finish_backend_failure(qs):
    task = qs.submit(_quick().error("no luck"), user="alice")
    qs.wait_for_failed(task)


def _finish_invalid_payload(qs):
    task = qs.submit(None, payload=b"not a test task", user="alice")
    qs.wait_for_failed(task)


def _finish_cancel_running(qs):
    _end(qs, _long_running(qs, "alice"))


def _finish_cancel_before_start(qs):
    task = qs.submit(TT().submit_time(LONG), user="alice")
    time.sleep(0.2)
    _end(qs, task)


@pytest.mark.parametrize(
    "finish",
    [
        _finish_ok,
        _finish_backend_failure,
        _finish_invalid_payload,
        _finish_cancel_running,
        _finish_cancel_before_start,
    ],
    ids=["finished", "backend_failure", "invalid_payload", "running", "not_started"],
)
def test_task_stops_counting_when_it_ends(qscheduler_test, finish):
    """A task left counted as in the backend would be priced far above Bob's usage."""
    qs = qscheduler_test
    _start(qs)
    _run(qs, "bob", 1.0)
    finish(qs)
    _alice_goes_before_bob(qs)


# Usage: what a task charges


def test_usage_is_execution_time_not_time_limit(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.6, time_limit_ms=10_000)
    _run(qs, "bob", 0.1, time_limit_ms=900_000)
    _bob_goes_before_alice(qs)


def test_failed_task_charges_execution_time(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    qs.wait_for_failed(qs.submit(TT().compute_time(0.5).error("x"), user="alice"))
    _bob_goes_before_alice(qs)


def test_cancelled_task_charges_execution_time(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    task = _long_running(qs, "alice")
    time.sleep(0.5)
    _end(qs, task)
    _bob_goes_before_alice(qs)


def test_usage_adds_up_over_tasks(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    for _ in range(3):
        _run(qs, "alice", 0.3)
    _run(qs, "bob", 0.6)
    _bob_goes_before_alice(qs)


# Decay


@pytest.mark.parametrize(
    "half_time, alice_goes_first",
    [("100ms", True), ("90m", False)],
    ids=["short_half_time", "long_half_time"],
)
def test_old_usage_decays(qscheduler_test, half_time, alice_goes_first):
    """Alice's usage is larger but older than Bob's, so the half-time decides who is lower."""
    qs = qscheduler_test
    _start(qs, decay_half_time=half_time)
    _run(qs, "alice", 1.0)
    time.sleep(1.0)
    _run(qs, "bob", 0.2)
    with machine_held(qs):
        alice = qs.submit(_quick(), user="alice")
        bob = qs.submit(_quick(), user="bob")
    if alice_goes_first:
        assert_start_order(qs, alice, bob)
    else:
        assert_start_order(qs, bob, alice)


def test_decay_does_not_touch_tasks_in_backend(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2, decay_half_time="10ms")
    free_slot = _long_running(qs, HOLDER)
    _long_running(qs, "alice")
    # Many half-times pass and decays run; a running task must still count in full.
    for _ in range(3):
        time.sleep(0.1)
        _end(qs, free_slot)
        free_slot = _long_running(qs, HOLDER)
    alice = qs.submit(_quick(), user="alice")
    dave = qs.submit(TT().compute_time(LONG), user="dave")
    _assert_next_is_dave(qs, free_slot, alice, dave)


def test_new_half_time_applies_after_reconfiguration(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 1.0)
    qs.api_update_machine(queue_size=1, decay_half_time_ms=100)
    time.sleep(1.0)
    _run(qs, "bob", 0.2)
    with machine_held(qs):
        alice = qs.submit(_quick(), user="alice")
        bob = qs.submit(_quick(), user="bob")
    assert_start_order(qs, alice, bob)


# Project limits


def test_tasks_failed_on_project_limit_are_not_counted(qscheduler_test):
    qs = qscheduler_test
    # Keep the periodic check out of it, so the tasks fail on being picked for the backend.
    _start(qs, limit_check_interval=60)
    qs.add_project("drained", limit_ms=100)
    _run(qs, "bob", 0.6)

    first = qs.submit(TT().compute_time(0.2), user="alice", project="drained")
    rejected = [qs.submit(_quick(), user="alice", project="drained") for _ in range(3)]
    qs.wait_for_finished(first)
    for task in rejected:
        qs.wait_for_failed(task)
        info = qs.get_task(task)
        assert info["error"] == "Project time limit exceeded"
        assert "started_at" not in info

    # Her tasks of a funded project aren't held back by the ones that never ran.
    _alice_goes_before_bob(qs)


def test_task_failed_by_running_project_limit_stops_counting(qscheduler_test):
    qs = qscheduler_test
    _start(qs, limit_check_interval=1)
    qs.add_project("drained", limit_ms=500)
    _run(qs, "bob", 2.5)
    task = qs.submit(TT().compute_time(LONG), user="alice", project="drained")
    qs.wait_for_failed(task)
    assert qs.get_task(task)["error"] == "Project time limit exceeded"
    assert qs.get_task(task)["exectime_ms"] < 2_500
    _alice_goes_before_bob(qs)


def test_project_of_task_does_not_matter_for_usage(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    qs.add_project("other", limit_ms=100_000)
    _run(qs, "alice", 0.5, project="other")
    _bob_goes_before_alice(qs)


# Suspensions


@pytest.mark.parametrize("cancel", [None, "waiting", "all"])
def test_usage_survives_suspension(qscheduler_test, cancel):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    qs.suspend_machine("maintenance", cancel=cancel)
    qs.resume_machine()
    _bob_goes_before_alice(qs)


def test_suspend_waiting_keeps_counting_running_tasks(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2)
    free_slot = _long_running(qs, HOLDER)
    running = _long_running(qs, "alice")
    queued = qs.submit(_quick(), user="alice")

    qs.suspend_machine("maintenance", cancel="waiting")
    qs.wait_for_failed(queued)
    assert qs.get_task_state(running) == "running"
    qs.resume_machine()

    alice = qs.submit(_quick(), user="alice")
    dave = qs.submit(TT().compute_time(LONG), user="dave")
    _assert_next_is_dave(qs, free_slot, alice, dave)


def test_suspend_waiting_fails_queued_items_of_all_users(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.1)
    hold = _long_running(qs, HOLDER)
    tasks = [qs.submit(_quick(), user=u) for u in ("alice", "bob", "alice", "carol")]
    sessions = [qs.new_session(5, owner=u) for u in ("alice", "bob")]

    qs.suspend_machine("maintenance", cancel="waiting")
    for task in tasks:
        qs.wait_for_failed(task)
    for session in sessions:
        qs.wait_for_session_closed(session)
    assert qs.get_task_state(hold) == "running"

    qs.resume_machine()
    _end(qs, hold)
    _run(qs, "alice", 0.01)


def test_running_task_finishing_after_suspend_waiting(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2)
    running = qs.submit(TT().compute_time(0.5), user="alice")
    qs.wait_for_running(running)
    qs.suspend_machine("maintenance", cancel="waiting")
    qs.wait_for_finished(running)
    qs.resume_machine()
    # Its execution time is charged, and it stops counting as a task in the backend.
    _run(qs, "bob", 0.1)
    _bob_goes_before_alice(qs)


def test_suspend_all_stops_counting_tasks(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2)
    _run(qs, "bob", 1.0)
    running = [_long_running(qs, "alice") for _ in range(2)]
    qs.suspend_machine("maintenance", cancel="all")
    for task in running:
        qs.wait_for_failed(task)
    qs.resume_machine()
    _alice_goes_before_bob(qs)


def test_suspend_all_charges_running_time(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    running = _long_running(qs, "alice")
    time.sleep(1.0)
    qs.suspend_machine("maintenance", cancel="all")
    qs.wait_for_failed(running)
    assert qs.get_task(running)["exectime_ms"] >= 1_000
    qs.resume_machine()
    _run(qs, "bob", 0.2)
    _bob_goes_before_alice(qs)


def test_suspend_all_stops_counting_not_started_task(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "bob", 0.5)
    task = qs.submit(TT().submit_time(LONG), user="alice")
    # Lets it reach the backend, where it stays before its start.
    time.sleep(0.2)
    qs.suspend_machine("maintenance", cancel="all")
    qs.wait_for_failed(task)
    assert "started_at" not in qs.get_task(task)
    qs.resume_machine()
    _alice_goes_before_bob(qs)


def test_suspend_all_ignores_late_backend_report(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "bob", 1.0)
    running = _long_running(qs, "alice")
    qs.suspend_machine("maintenance", cancel="all")
    qs.wait_for_failed(running)
    # The backend's report of the cancellation arrives by now; it's not charged a second time.
    time.sleep(0.3)
    qs.resume_machine()
    _alice_goes_before_bob(qs)


def test_suspend_all_with_session_tasks(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    session = qs.new_session(30, owner="alice")
    qs.wait_for_session_open(session)
    task = qs.submit(TT().compute_time(LONG), session_id=session, user="alice")
    qs.wait_for_running(task)
    time.sleep(0.5)

    qs.suspend_machine("maintenance", cancel="all")
    qs.wait_for_failed(task)
    qs.wait_for_session_closed(session)
    qs.resume_machine()

    # Alice is charged for the time her session held the machine.
    _run(qs, "bob", 0.2)
    _bob_goes_before_alice(qs)


# Restarts: tasks in the backend come back from the DB, usage doesn't


@pytest.mark.parametrize("kill", [True, False], ids=["crash", "graceful"])
def test_restart_restores_running_task(qscheduler_test, kill):
    qs = qscheduler_test
    _start(qs, queue_size=2)
    free_slot = _long_running(qs, HOLDER)
    _long_running(qs, "alice")
    alice = qs.submit(_quick(), user="alice")
    dave = qs.submit(TT().compute_time(LONG), user="dave")

    qs.restart(kill=kill)
    _assert_next_is_dave(qs, free_slot, alice, dave)


def test_restart_resumed_task_stops_counting_when_it_ends(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    task = _long_running(qs, "alice")
    qs.restart()
    _end(qs, task)
    _run(qs, "bob", 0.5)
    _alice_goes_before_bob(qs)


def test_restart_does_not_count_session_task(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    session = qs.new_session(30, owner="alice")
    qs.wait_for_session_open(session)
    task = _long_running(qs, "alice", session_id=session)
    qs.restart()
    # A restart closes open sessions and cancels their tasks.
    qs.wait_for_cancelled(task)
    qs.wait_for_session_closed(session)
    _run(qs, "bob", 0.3)
    _alice_goes_before_bob(qs)


def test_restart_keeps_users_taking_turns(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    hold = _long_running(qs, HOLDER)
    tasks = {}
    for user in ("alice", "bob"):
        for _ in range(3):
            tasks[qs.submit(TT().compute_time(0.1), user=user)] = user
    qs.restart()
    _end(qs, hold)
    for task in tasks:
        qs.wait_for_finished(task)
    order = [tasks[t] for t in sorted(tasks, key=lambda t: _started_at(qs, t))]
    assert order != ["alice"] * 3 + ["bob"] * 3
    assert order != ["bob"] * 3 + ["alice"] * 3


def _reconfigure(qs):
    qs.api_update_machine(queue_size=1)


@pytest.mark.parametrize("reload", [QScheduler.restart, _reconfigure])
def test_reload_keeps_order_of_tasks_of_a_user(qscheduler_test, reload):
    qs = qscheduler_test
    _start(qs)
    hold = _long_running(qs, HOLDER)
    tasks = [qs.submit(_quick(), user="alice") for _ in range(6)]
    reload(qs)
    _end(qs, hold)
    assert_start_order(qs, *tasks)


@pytest.mark.parametrize("reload", [QScheduler.restart, _reconfigure])
def test_reload_keeps_order_of_sessions_of_a_user(qscheduler_test, reload):
    qs = qscheduler_test
    _start(qs)
    hold = _long_running(qs, HOLDER)
    sessions = [qs.new_session(30, owner="alice") for _ in range(4)]
    reload(qs)
    _end(qs, hold)
    for i, session in enumerate(sessions):
        qs.wait_for_session_open(session)
        for later in sessions[i + 1 :]:
            assert qs.get_session_state(later) == "waiting"
        qs.cancel_session(session)
        qs.wait_for_session_closed(session)


def test_restart_resets_usage(qscheduler_test):
    """Usage is kept in memory only, so a restart forgets it. If it gets persisted one day, Bob
    should go first here instead."""
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    qs.restart()
    with machine_held(qs):
        alice = qs.submit(_quick(), user="alice")
        bob = qs.submit(_quick(), user="bob")
    assert_start_order(qs, alice, bob)


# Reconfiguration


def test_reconfiguration_keeps_usage(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    qs.api_update_machine(queue_size=1)
    _bob_goes_before_alice(qs)


def test_repeated_reconfiguration_keeps_usage(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    for queue_size in (3, 2, 1):
        qs.api_update_machine(queue_size=queue_size)
    _bob_goes_before_alice(qs)


def test_reconfiguration_keeps_usage_of_queued_users(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    hold = _long_running(qs, HOLDER)
    alice = qs.submit(_quick(), user="alice")
    bob = qs.submit(_quick(), user="bob")
    qs.api_update_machine(queue_size=1)
    _end(qs, hold)
    assert_start_order(qs, bob, alice)


def test_reconfiguration_restores_tasks_in_backend(qscheduler_test):
    qs = qscheduler_test
    _start(qs, queue_size=2)
    free_slot = _long_running(qs, HOLDER)
    _long_running(qs, "alice")
    alice = qs.submit(_quick(), user="alice")
    dave = qs.submit(TT().compute_time(LONG), user="dave")
    qs.api_update_machine(queue_size=2)
    _assert_next_is_dave(qs, free_slot, alice, dave)


def test_reconfiguration_does_not_duplicate_tasks_in_backend(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    task = _long_running(qs, "alice")
    qs.api_update_machine(queue_size=1)
    qs.api_update_machine(queue_size=1)
    _end(qs, task)
    _run(qs, "bob", 0.5)
    _alice_goes_before_bob(qs)


def test_reconfiguration_with_nothing_running_adds_no_tasks(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "bob", 1.0)
    _run(qs, "alice", 0.1)
    qs.api_update_machine(queue_size=1)
    _alice_goes_before_bob(qs)


# Sessions: the owner is charged for the time held, not for the tasks; they queue by owner's usage


def test_idle_session_charges_time_held_to_owner(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    session = qs.new_session(30, owner="alice")
    qs.wait_for_session_open(session)
    time.sleep(0.5)
    qs.cancel_session(session)
    qs.wait_for_session_closed(session)
    _run(qs, "bob", 0.2)
    _bob_goes_before_alice(qs)


def test_session_task_does_not_set_average(qscheduler_test):
    """With its 0.3s as her average, Alice's running task would be cheap, and she'd go first."""
    qs = qscheduler_test
    _start(qs, queue_size=2)
    session = qs.new_session(30, owner="alice")
    qs.wait_for_session_open(session)
    _run(qs, "alice", 0.3, session_id=session)
    qs.cancel_session(session)
    qs.wait_for_session_closed(session)
    _run(qs, "bob", 1.0)
    free_slot = _long_running(qs, HOLDER)
    _long_running(qs, "alice")
    alice = qs.submit(_quick(), user="alice")
    bob = qs.submit(TT().compute_time(LONG), user="bob")
    _end(qs, free_slot)
    qs.wait_for_running(bob)
    assert qs.get_task_state(alice) == "waiting"


def test_session_closed_by_restart_charges_time_held_to_owner(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    session = qs.new_session(30, owner="alice")
    qs.wait_for_session_open(session)
    time.sleep(0.5)
    # A graceful stop checkpoints the time held; a crash loses it back to the last checkpoint.
    qs.restart(kill=False)
    qs.wait_for_session_closed(session)
    _run(qs, "bob", 0.2)
    _bob_goes_before_alice(qs)


def test_session_that_never_opened_is_not_charged(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    with machine_held(qs):
        session = qs.new_session(30, owner="alice")
        time.sleep(0.5)
        qs.cancel_session(session)
        qs.wait_for_session_closed(session)
    _run(qs, "bob", 0.2)
    _alice_goes_before_bob(qs)


def test_session_of_light_user_goes_before_task_of_heavy_user(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    with machine_held(qs):
        task = qs.submit(_quick(), user="alice")
        session = qs.new_session(30, owner="bob")
    qs.wait_for_session_open(session)
    assert qs.get_task_state(task) == "waiting"
    qs.cancel_session(session)
    qs.wait_for_finished(task)


def test_session_of_heavy_user_goes_after_task_of_light_user(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    with machine_held(qs):
        session = qs.new_session(30, owner="alice")
        task = qs.submit(TT().compute_time(0.5), user="bob")
    qs.wait_for_running(task)
    assert qs.get_session_state(session) == "waiting"
    qs.wait_for_finished(task)
    qs.wait_for_session_open(session)


def test_session_next_in_line_holds_back_other_users(qscheduler_test):
    """A session needs an empty backend; tasks of users behind it don't overtake it meanwhile,
    even if a slot is free."""
    qs = qscheduler_test
    _start(qs, queue_size=2)
    _run(qs, "alice", 0.3)
    hold = _long_running(qs, HOLDER)
    session = qs.new_session(30, owner="bob")
    task = qs.submit(_quick(), user="alice")
    time.sleep(0.3)
    assert qs.get_task_state(task) == "waiting"
    assert qs.get_session_state(session) == "waiting"

    _end(qs, hold)
    qs.wait_for_session_open(session)
    assert qs.get_task_state(task) == "waiting"

    qs.cancel_session(session)
    qs.wait_for_finished(task)


def test_cancelled_queued_session_does_not_hold_back_others(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    with machine_held(qs):
        session = qs.new_session(30, owner="bob")
        task = qs.submit(_quick(), user="alice")
        qs.cancel_session(session)
        qs.wait_for_session_closed(session)
    qs.wait_for_finished(task)


def test_tasks_in_session_run_regardless_of_usage(qscheduler_test):
    qs = qscheduler_test
    _start(qs)
    _run(qs, "alice", 0.5)
    session = qs.new_session(30, owner="alice")
    qs.wait_for_session_open(session)
    # Waiting outside of the session while it's open, although Bob's usage is lower.
    outside = qs.submit(_quick(), user="bob")
    inside = qs.submit(_quick(), user="alice", session_id=session)
    qs.wait_for_finished(inside)
    assert qs.get_task_state(outside) == "waiting"
    qs.cancel_session(session)
    qs.wait_for_finished(outside)
