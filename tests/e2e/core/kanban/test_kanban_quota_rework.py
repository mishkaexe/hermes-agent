"""Live dispatcher recovery through repeated fake-provider quota failures and review rework."""

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.e2e.core.kanban._helpers import Board, wait_until
from tests.e2e.core.kanban.test_kanban_rate_limit_review import REVIEW_MARK, _seed_review_skill
from tests.fakes.fake_llm_provider import Error, FakeLLMServer, Text, ToolCall

pytestmark = [pytest.mark.platforms("linux"), pytest.mark.live_system_guard_bypass]


class RecoveryVendor:
    def __init__(self):
        self.tick = 0
        self.tid = ""
        self.attempts = []

    def __call__(self, request):
        messages = request["body"].get("messages", [])
        role = "review" if REVIEW_MARK in json.dumps(messages) else "impl"
        if not self.attempts or self.attempts[-1]["tick"] != self.tick:
            self.attempts.append({"tick": self.tick, "role": role, "answers": []})
        attempt = self.attempts[-1]
        assert attempt["role"] == role
        attempts = [a for a in self.attempts if a["role"] == role]
        if role == "impl" and len(attempts) <= 2:
            reply = Error(status=429, message="Rate limit exceeded: quota for this key", retry_after=0)
        elif role == "review" and len(attempts) == 1 and not attempt["answers"]:
            reply = ToolCall("kanban_comment", {"task_id": self.tid, "body": "Review https://github.com/example/repo/pull/1"})
        elif role == "review" and len(attempts) == 1 and len(attempt["answers"]) == 1:
            reply = ToolCall("kanban_request_changes", {"reason": "correct the existing PR"})
        elif not attempt["answers"] and role == "impl":
            reply = ToolCall("kanban_request_review", {"summary": "candidate ready", "reviewer": "reviewer"})
        elif not attempt["answers"] and role == "review":
            reply = ToolCall("kanban_complete", {"summary": "review accepted"})
        else:
            reply = Text("done")
        attempt["answers"].append(reply.name if isinstance(reply, ToolCall) else type(reply).__name__)
        return reply


def test_repeated_quota_then_review_rework_keeps_one_worktree(tmp_path: Path):
    model = RecoveryVendor()
    with FakeLLMServer(model) as server:
        board = Board(tmp_path, server.base_url, env_extra={"HERMES_KANBAN_RATE_LIMIT_COOLDOWN_SECONDS": "2"})
        _seed_review_skill(board)
        profile = subprocess.run([sys.executable, "-m", "hermes_cli.main", "profile", "create", "reviewer", "--clone", "--no-alias"],
                                 env=board.env(), cwd=tmp_path, capture_output=True, text=True, timeout=90, check=False)
        assert profile.returncode == 0, profile.stderr
        repo = tmp_path / "repo"
        repo.mkdir()
        for cmd in (["git", "init"], ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "base"]):
            subprocess.run(cmd, cwd=repo, check=True, capture_output=True, timeout=30)
        tid = board.create("recovery cycle", "--workspace", f"worktree:{repo}")
        model.tid = tid
        try:
            for tick in range(1, 10):
                model.tick = tick
                result = board.dispatch()
                spawns = [s for s in result.get("spawned", []) if s["task_id"] == tid]
                assert len(spawns) <= 1, board.diag(tid)
                if spawns:
                    pid = board.events(tid, "spawned")[-1]["payload"]["pid"]
                    board.wait_worker_exit(tid, int(pid))
                task = board.task(tid)
                assert task["consecutive_failures"] == 0, board.diag(tid)
                if task["status"] == "done":
                    break
                if any(g.get("reason") == "rate_limit_cooldown" for g in result.get("respawn_guarded", [])):
                    # Dispatch is a separate process on each pass; reopen persisted state after cooldown.
                    ended = board.runs(tid)[-1]["ended_at"]
                    wait_until(lambda: time.time() >= ended + 2, 10, "quota cooldown")
            assert board.task(tid)["status"] == "done", board.diag(tid)
            assert [r["outcome"] for r in board.runs(tid)] == [
                "rate_limited", "rate_limited", "review_requested", "changes_requested", "review_requested", "completed"]
            assert [a["role"] for a in model.attempts] == ["impl", "impl", "impl", "review", "impl", "review"]
            assert len(board.events(tid, "rate_limited")) == 2
            assert len(board.events(tid, "changes_requested")) == 1
            assert len(board.events(tid, "commented")) == 1
            assert not board.events(tid, "gave_up")
            worktree = Path(board.task(tid)["workspace_path"])
            assert worktree.is_dir() and (worktree / ".git").is_file()
        finally:
            board.kill_workers()
