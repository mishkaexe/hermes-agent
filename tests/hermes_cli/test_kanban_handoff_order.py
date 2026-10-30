"""Same-second review handoffs retain duplicate-PR protection."""

from pathlib import Path
import time

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
