"""Regression coverage for quota diagnostics across review handoffs (#119070)."""

from pathlib import Path

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
    kb.init_db()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="quota recovery", assignee="dev")
        claim = kb.claim_task(conn, tid)
        if handoff == "changes_requested":
            assert kb.request_review(conn, tid, summary="ready", reviewer="reviewer", expected_run_id=claim.current_run_id)
            claim = kb.claim_review_task(conn, tid)
        # Reproduce the diagnostic retained by a previous quota-limited attempt.
        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET last_failure_error = ? WHERE id = ?", (
                "pid 1 exited rate-limited (quota wall) — requeued without counting a failure", tid))
        if handoff == "review_requested":
            assert kb.request_review(conn, tid, summary="ready", reviewer="reviewer", expected_run_id=claim.current_run_id)
            lane = "review"
        else:
            ok, _ = kb.request_changes(conn, tid, reason="correct it", expected_run_id=claim.current_run_id)
            assert ok
            lane = "ready"
        assert kbd.check_respawn_guard(conn, tid, lane=lane) is None
