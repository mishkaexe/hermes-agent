"""Owner-private durable launch fences for finite Kanban admissions."""
import json

from hermes_state_local import POLICY_PREFIX
from hermes_state_local_lineage import local_physical_target
from hermes_state_runtime import RuntimeStoreError, _admission, _epoch, _json, _session

PREFIX = 'gateway.kanban.launch.v1:'


def establish_launch_fence(authority, ref, row):
    """Record that this exact started attempt has not authorized a worker launch.

    A missing legacy record is never proof of non-launch. This operation runs before
    ingress; it must not replace an existing record or turn a prior request into proof.
    """
    def write(conn):
        _epoch(conn, authority.epoch)
        raw = conn.execute('SELECT value FROM state_meta WHERE key=?', (POLICY_PREFIX + ref.session_id,)).fetchone()
        if raw is None:
            return
        receipt = json.loads(raw[0])
        policy = receipt.get('policy', {})
        if policy.get('source') != 'kanban':
            return
        request = json.loads(policy['request_json'])
        attempt = [request['board'], request['task_id'], request['run_id']]
        expected = 'kanban:' + json.dumps(attempt, separators=(',', ':'))
        admission = _admission(conn, row['admission_id'])
        session = _session(conn, ref.session_id)
        if (receipt['profile_id'] != authority.profile_id or receipt['session_id'] != ref.session_id
                or receipt['request_id'] != expected or admission['request_id'] != expected
                or admission['target_session_id'] != ref.session_id or admission['status'] != 'started'
                or admission['owner_epoch'] != authority.epoch or admission['generation'] != row['generation']
                or session['runtime_generation'] != row['generation']):
            raise RuntimeStoreError('stale_generation')
        target = local_physical_target(conn, ref.session_id)
        if _session(conn, target)['ended_at'] is not None:
            raise RuntimeStoreError('stale_generation')
        key = PREFIX + row['admission_id']
        record = dict(version=1, state='not_launched', profile_id=authority.profile_id,
                      session_id=ref.session_id, target_id=target, admission_id=row['admission_id'],
                      generation=row['generation'], epoch=authority.epoch, attempt=attempt)
        old = conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()
        if old is not None:
            if json.loads(old[0]) != record:
                raise RuntimeStoreError('stale_generation')
            return
        if conn.execute('SELECT 1 FROM worker_executions WHERE session_id IN (?,?) AND generation=?',
                        (ref.session_id, target, row['generation'])).fetchone():
            raise RuntimeStoreError('stale_generation')
        conn.execute('INSERT INTO state_meta(key,value) VALUES(?,?)', (key, _json(record)))
    return authority.db._execute_write(write)


def authorize_launch(authority, ref, row, *, required=False):
    """Persist the launch permit before scheduling process acquisition; refuse stale permits."""
    def write(conn):
        _epoch(conn, authority.epoch)
        saved = conn.execute('SELECT value FROM state_meta WHERE key=?', (POLICY_PREFIX + ref.session_id,)).fetchone()
        if saved is None or json.loads(saved[0]).get('policy', {}).get('source') != 'kanban':
            if required or conn.execute('SELECT 1 FROM state_meta WHERE key=?', (PREFIX + row['admission_id'],)).fetchone():
                raise RuntimeStoreError('storage_unavailable')
            return
        admission = _admission(conn, row['admission_id'])
        key = PREFIX + row['admission_id']
        stored = conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()
        if stored is None:
            raise RuntimeStoreError('stale_generation')
        record = json.loads(stored[0])
        target = local_physical_target(conn, ref.session_id)
        if (record.get('state') != 'not_launched' or record.get('epoch') != authority.epoch
                or record.get('session_id') != ref.session_id or record.get('target_id') != target
                or record.get('admission_id') != row['admission_id'] or record.get('profile_id') != authority.profile_id
                or record.get('generation') != row['generation'] or admission['status'] != 'started'
                or admission['owner_epoch'] != authority.epoch or admission['generation'] != row['generation']
                or _session(conn, ref.session_id)['runtime_generation'] != row['generation']
                or _session(conn, target)['ended_at'] is not None):
            raise RuntimeStoreError('stale_generation')
        record['state'] = 'launch_requested'
        conn.execute('UPDATE state_meta SET value=? WHERE key=?', (_json(record), key))
    return authority.db._execute_write(write)


def retain_verified_identity(conn, authority, admission, target, pid, birth):
    """Commit verified interpreter identity in the reservation's own transaction."""
    owner = admission['target_session_id']
    saved = conn.execute('SELECT value FROM state_meta WHERE key=?', (POLICY_PREFIX + owner,)).fetchone()
    key = PREFIX + admission['admission_id']
    stored = conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()
    if saved is None or json.loads(saved[0]).get('policy', {}).get('source') != 'kanban':
        if stored is not None:
            raise RuntimeStoreError('storage_unavailable')
        return
    if stored is None:
        raise RuntimeStoreError('stale_generation')
    record = json.loads(stored[0])
    if (record.get('state') != 'launch_requested' or record.get('epoch') != authority.epoch
            or record.get('profile_id') != authority.profile_id or record.get('session_id') != owner
            or record.get('target_id') != target or record.get('admission_id') != admission['admission_id']
            or record.get('generation') != admission['generation'] or _session(conn, target)['ended_at'] is not None):
        raise RuntimeStoreError('stale_generation')
    record.update(state='identified', pid=pid, birth=birth)
    conn.execute('UPDATE state_meta SET value=? WHERE key=?', (_json(record), key))
