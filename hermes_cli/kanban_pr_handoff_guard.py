"""Duplicate-PR guard with transactional review-handoff ordering."""


def active_pr_guard(conn, task_id, now):
    # Late import preserves the dispatcher's existing helper/patch seams.
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_dispatch as dispatch

    comments = [(t, c["id"]) for t, c in dispatch._rows_since(
        conn, "task_comments", task_id, now - dispatch._RESPAWN_GUARD_PR_WINDOW,
        cols=", id, body", unreadable=now)
        if dispatch._RESPAWN_GUARD_PR_URL_RE.search(kb._lossy_text(c["body"]) or "")]
    if not comments:
        return None
    newest = max(t for t, _ in comments)
    events = dispatch._rows_since(
        conn, "task_events", task_id, newest,
        "AND kind IN ('assigned', 'changes_requested', 'review_reopened')",
        cols=", kind, payload", strict=True)
    if any(dispatch._is_handoff_event(e["kind"], e["payload"]) for _, e in events):
        return None
    # The handoff write lock captures a comment-table watermark. It proves
    # ordering on a tied second without comparing unrelated table sequences.
    assignee = conn.execute("SELECT assignee FROM tasks WHERE id = ?", (task_id,)).fetchone()[0]
    for timestamp, event in dispatch._rows_since(
            conn, "task_events", task_id, newest,
            "AND kind = 'changes_requested'", cols=", payload"):
        data = kb._json_or(event["payload"], {})
        if (timestamp == newest and isinstance(data, dict)
                and type(data.get("comment_watermark")) is int
                and data["comment_watermark"] >= max(cid for _, cid in comments)
                and assignee and data.get("implementer") == assignee):
            return None
    return "active_pr"
