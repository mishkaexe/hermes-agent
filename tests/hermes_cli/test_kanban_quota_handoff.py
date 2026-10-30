"""Regression coverage for quota diagnostics across review handoffs (#119070)."""

from pathlib import Path
import time

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_dispatch as kbd


@pytest.mark.parametrize("handoff", ["review_requested", "changes_requested"])
def test_successful_handoff_supersedes_stale_quota_stamp(tmp_path, monkeypatch, handoff):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    monkeypatch.setenv("HERMES_KANBAN_RATE_LIMIT_COOLDOWN_SECONDS", "300")
    kb.init_db()
    clock = [time.time()]
    monkeypatch.setattr(kb.time, "time", lambda: clock[0])
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="quota recovery", assignee="dev")
        claim = kb.claim_task(conn, tid)
        if handoff == "changes_requested":
            assert kb.request_review(conn, tid, summary="ready", reviewer="reviewer", expected_run_id=claim.current_run_id)
            claim = kb.claim_review_task(conn, tid)
        # Real reaper books a neutral quota-limited run in this isolated board.
        clock[0] += 1
        pid = 7654321
        kbd._set_worker_pid(conn, tid, pid)
        kbd._record_worker_exit(pid, kb.KANBAN_RATE_LIMIT_EXIT_CODE << 8)
        kbd.detect_crashed_workers(conn)
        assert kb.get_task(conn, tid).consecutive_failures == 0
        assert kbd.check_respawn_guard(conn, tid, lane="review" if handoff == "changes_requested" else "ready") == "rate_limit_cooldown"
        # Let the bounded cooldown expire, then use the normal lifecycle APIs.
        clock[0] += 301
        claim = kb.claim_review_task(conn, tid) if handoff == "changes_requested" else kb.claim_task(conn, tid)
        if handoff == "review_requested":
            assert kb.request_review(conn, tid, summary="ready", reviewer="reviewer", expected_run_id=claim.current_run_id)
            lane = "review"
        else:
            ok, _ = kb.request_changes(conn, tid, reason="correct it", expected_run_id=claim.current_run_id)
            assert ok
            lane = "ready"
        assert kbd.check_respawn_guard(conn, tid, lane=lane) is None
