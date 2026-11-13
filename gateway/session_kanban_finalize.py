"""Owner-side finite Kanban settlement; uncertain launches remain open."""
import json
import time

from gateway.session_kanban_launch import PREFIX
from hermes_state_local import POLICY_PREFIX
from hermes_state_local_lineage import local_physical_target
from hermes_state_runtime import _epoch, _session

REASON = 'kanban_attempt_terminal'


def end_unlaunched_attempt(authority, session_id):
    """Conditionally end one terminal attempt with explicit never-launched proof."""
    def write(conn):
        _epoch(conn, authority.epoch)
        saved = conn.execute('SELECT value FROM state_meta WHERE key=?', (POLICY_PREFIX + session_id,)).fetchone()
        if saved is None:
            return None
        receipt = json.loads(saved[0])
        if receipt.get('profile_id') != authority.profile_id or receipt.get('session_id') != session_id:
            return None
        if receipt.get('policy', {}).get('source') != 'kanban':
            return None
        request = json.loads(receipt['policy']['request_json'])
        attempt = [request['board'], request['task_id'], request['run_id']]
        request_id = 'kanban:' + json.dumps(attempt, separators=(',', ':'))
        if receipt['request_id'] != request_id:
            return None
        admission = conn.execute('SELECT * FROM session_admissions WHERE target_session_id=? AND request_id=?',
                                 (session_id, request_id)).fetchone()
        if admission is None or admission['status'] != 'terminal':
            return None
        stored = conn.execute('SELECT value FROM state_meta WHERE key=?', (PREFIX + admission['admission_id'],)).fetchone()
        if stored is None:
            return None
        record = json.loads(stored[0])
        target = local_physical_target(conn, session_id)
        if (record.get('state') != 'not_launched' or record.get('profile_id') != authority.profile_id
                or record.get('session_id') != session_id or record.get('target_id') != target
                or record.get('attempt') != attempt or record.get('admission_id') != admission['admission_id']
                or record.get('version') != 1 or record.get('epoch') != admission['owner_epoch']
                or record.get('generation') != admission['generation']
                or _session(conn, session_id)['runtime_generation'] != admission['generation']):
            return None
        if conn.execute("SELECT 1 FROM session_admissions WHERE target_session_id=? AND status IN ('queued','started','unknown')",
                        (session_id,)).fetchone():
            return None
        if conn.execute("SELECT 1 FROM worker_executions WHERE session_id IN (?,?) AND status IN ('registered','running','unknown')",
                        (session_id, target)).fetchone():
            return None
        changed = authority.db._end_and_bump(conn,
            'UPDATE sessions SET ended_at=?, end_reason=? WHERE id=? AND ended_at IS NULL',
            (time.time(), REASON, target), target, REASON)
        return target if changed else None
    return authority.db._execute_write(write)


async def settle_unlaunched_attempt(authority, ref, live):
    """Dispatch only for the fresh end winner; recovery never replays a committed end."""
    from functools import partial
    from gateway.session_runtime_workers import tracked_write
    target = await tracked_write(authority, partial(end_unlaunched_attempt, authority, ref.session_id))
    if target is not None:
        finalize = getattr(authority.runner, '_finalize_session_off_loop', None)
        if finalize is not None:
            await finalize(session_id=target, platform='cli', reason=REASON, session_key=live.route)
