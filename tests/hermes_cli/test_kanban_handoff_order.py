"""Same-second review handoffs retain duplicate-PR protection."""

from pathlib import Path
import json
import time

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_dispatch as kbd


def test_review_return_after_same_second_pr_comment(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    now = time.time()
    monkeypatch.setattr(kb.time, "time", lambda: now)
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="existing PR", assignee="dev")
        impl = kb.claim_task(conn, tid)
        assert kb.request_review(conn, tid, summary="ready", reviewer="reviewer", expected_run_id=impl.current_run_id)
        review = kb.claim_review_task(conn, tid)
        kb.add_comment(conn, tid, "reviewer", "Review https://github.com/example/repo/pull/1")
        ok, implementer = kb.request_changes(conn, tid, reason="correct it", expected_run_id=review.current_run_id)
        assert ok and implementer == "dev"
        assert kbd.check_respawn_guard(conn, tid) is None
        kb.add_comment(conn, tid, "dev", "Another https://github.com/example/repo/pull/2")
        assert kbd.check_respawn_guard(conn, tid) == "active_pr"


@pytest.mark.parametrize("watermark", [None, True, "999999", -1, 999999])
def test_ambiguous_handoff_watermark_fails_closed(tmp_path, monkeypatch, watermark):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    now = time.time()
    monkeypatch.setattr(kb.time, "time", lambda: now)
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="existing PR", assignee="dev")
        impl = kb.claim_task(conn, tid)
        assert kb.request_review(conn, tid, reviewer="reviewer", expected_run_id=impl.current_run_id)
        review = kb.claim_review_task(conn, tid)
        kb.add_comment(conn, tid, "reviewer", "https://github.com/example/repo/pull/1")
        assert kb.request_changes(conn, tid, reason="fix", expected_run_id=review.current_run_id)[0]
        payload = {"implementer": "dev", "comment_watermark": watermark}
        with kb.write_txn(conn):
            conn.execute("UPDATE task_events SET payload = ? WHERE task_id = ? AND kind = 'changes_requested'", (json.dumps(payload), tid))
        assert kbd.check_respawn_guard(conn, tid) == "active_pr"
