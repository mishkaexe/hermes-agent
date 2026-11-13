"""Finite Kanban sessions end at owner settlement, independently of their viewer."""
import asyncio
from types import MethodType

import pytest

from tests.gateway.test_kanban_finite_admission import _CONFIG

pytest_plugins = ('tests.gateway.test_kanban_finite_admission',)


@pytest.mark.asyncio
async def test_finite_attempt_has_durable_no_launch_fence_before_ingress(kanban_owner, monkeypatch):
    import json
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task

    owner = kanban_owner(_CONFIG)
    authority = owner.authority
    observed = []

    async def execute(authority, ref, row):
        raw = authority.db.get_meta('gateway.kanban.launch.v1:' + row['admission_id'])
        observed.append(json.loads(raw) if raw else None)
        raise RuntimeError('synthetic admission execution failure')

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    try:
        sid = (await run_task(owner.connection, owner.params))['session_id']
        await authority._drain(SessionRef(authority.profile_id, sid))
        assert len(observed) == 1
        assert observed[0] is not None
        assert observed[0]['state'] == 'not_launched'
        assert observed[0]['session_id'] == sid
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_spawn_is_authorized_durably_before_process_acquisition(kanban_owner, monkeypatch):
    import json
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from gateway.session_managed_worker import _spawn_registered_worker

    owner = kanban_owner(_CONFIG)
    observed = []

    async def acquire(*args, **kwargs):
        raw = owner.authority.db.get_meta('gateway.kanban.launch.v1:' + admission_id[0])
        observed.append(json.loads(raw)['state'])
        raise RuntimeError('synthetic acquisition failure')

    admission_id = []
    async def execute(authority, ref, row):
        admission_id.append(row['admission_id'])
        return await _spawn_registered_worker(authority, ref, row)

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    monkeypatch.setattr('gateway.session_worker_spawn.acquire_process', acquire)
    try:
        sid = (await run_task(owner.connection, owner.params))['session_id']
        await owner.authority._drain(SessionRef(owner.authority.profile_id, sid))
        assert observed == ['launch_requested']
        assert owner.authority.db.get_session(sid)['ended_at'] is None
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_missing_receipt_cannot_authorize_kanban_worker_spawn(kanban_owner, monkeypatch):
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from gateway.session_managed_worker import _spawn_registered_worker
    from hermes_state_local import POLICY_PREFIX

    owner = kanban_owner(_CONFIG)
    spawned = []

    async def acquire(*args, **kwargs):
        spawned.append(True)
        raise RuntimeError('unexpected launch')

    async def execute(authority, ref, row):
        authority.db._execute_write(lambda conn: conn.execute(
            'DELETE FROM state_meta WHERE key=?', (POLICY_PREFIX + ref.session_id,)))
        return await _spawn_registered_worker(authority, ref, row)

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    monkeypatch.setattr('gateway.session_worker_spawn.acquire_process', acquire)
    try:
        sid = (await run_task(owner.connection, owner.params))['session_id']
        await owner.authority._drain(SessionRef(owner.authority.profile_id, sid))
        assert spawned == []
        assert owner.authority.db.get_session(sid)['ended_at'] is None
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_missing_receipt_cannot_reserve_or_bootstrap_worker(kanban_owner, monkeypatch):
    import os
    import subprocess
    import sys
    import psutil
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from gateway.session_kanban_launch import authorize_launch
    from gateway.session_worker_reservation import reserve_admission_worker
    from hermes_cli.process_identity import worker_birth
    from hermes_state_local import POLICY_PREFIX
    from hermes_state_runtime import RuntimeStoreError

    owner = kanban_owner(_CONFIG)
    attempts = []

    async def execute(authority, ref, row):
        await asyncio.to_thread(authorize_launch, authority, ref, row)
        process = await asyncio.to_thread(subprocess.Popen, [sys.executable, '-c', 'import sys; sys.stdin.read()'],
                                          stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            hello = {'type': 'hello', 'pid': process.pid, 'birth': worker_birth(psutil.Process(process.pid)),
                     'ancestors': [os.getpid()]}
            authority.db._execute_write(lambda conn: conn.execute(
                'DELETE FROM state_meta WHERE key=?', (POLICY_PREFIX + ref.session_id,)))
            try:
                await asyncio.to_thread(reserve_admission_worker, authority, admission_id=row['admission_id'],
                                        process=process, principal_id=row['principal_id'], hello=hello)
            except RuntimeStoreError:
                attempts.append('refused')
            else:
                attempts.append('reserved')
        finally:
            process.stdin.close()
            await asyncio.to_thread(process.wait, timeout=5)
        return ''

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    try:
        sid = (await run_task(owner.connection, owner.params))['session_id']
        await owner.authority._drain(SessionRef(owner.authority.profile_id, sid))
        assert attempts == ['refused']
        assert owner.authority.db._read_one('SELECT COUNT(*) FROM worker_executions')[0] == 0
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_restart_reschedules_settled_unlaunched_attempt_with_empty_fifo(kanban_owner, monkeypatch):
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from gateway.session_local_recovery import recover_local_sessions
    from hermes_state_runtime import begin_runtime_epoch

    owner = kanban_owner(_CONFIG)
    authority = owner.authority
    scheduled = []

    async def execute(*args):
        raise RuntimeError('synthetic admission execution failure')

    async def defer(*args):
        return None

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    try:
        sid = (await run_task(owner.connection, owner.params))['session_id']
        with monkeypatch.context() as patch:
            patch.setattr('gateway.session_kanban_finalize.settle_unlaunched_attempt', defer)
            await authority._drain(SessionRef(authority.profile_id, sid))
        assert authority.db.get_session(sid)['ended_at'] is None
        authority.epoch = begin_runtime_epoch(authority.db, instance_id='new-owner')
        monkeypatch.setattr(authority, '_schedule', lambda ref: scheduled.append(ref.session_id))
        recover_local_sessions(authority, schedule=True)
        assert sid in scheduled
        await authority._drain(SessionRef(authority.profile_id, sid))
        assert authority.db.get_session(sid)['end_reason'] == 'kanban_attempt_terminal'
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_pending_acquisition_without_worker_reservation_cannot_finalize(kanban_owner, monkeypatch):
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from gateway.session_kanban_finalize import end_unlaunched_attempt
    from gateway.session_managed_worker import _spawn_registered_worker

    owner = kanban_owner(_CONFIG)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def acquire(*args, **kwargs):
        entered.set()
        await release.wait()
        raise RuntimeError('synthetic late acquisition failure')

    async def execute(authority, ref, row):
        return await _spawn_registered_worker(authority, ref, row)

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    monkeypatch.setattr('gateway.session_worker_spawn.acquire_process', acquire)
    try:
        sid = (await run_task(owner.connection, owner.params))['session_id']
        drain = asyncio.create_task(owner.authority._drain(SessionRef(owner.authority.profile_id, sid)))
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert end_unlaunched_attempt(owner.authority, sid) is None
        assert owner.authority.db.get_session(sid)['ended_at'] is None
        release.set()
        await asyncio.wait_for(drain, timeout=5)
        assert end_unlaunched_attempt(owner.authority, sid) is None
        assert owner.authority.db.get_session(sid)['ended_at'] is None
    finally:
        release.set()
        await owner.close()


@pytest.mark.asyncio
async def test_verified_interpreter_identity_is_retained_with_reservation(kanban_owner, monkeypatch):
    import json
    import os
    import subprocess
    import sys
    import psutil
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from gateway.session_kanban_launch import authorize_launch
    from gateway.session_worker_reservation import reserve_admission_worker
    from hermes_cli.process_identity import worker_birth

    owner = kanban_owner(_CONFIG)
    observed = []

    async def execute(authority, ref, row):
        await asyncio.to_thread(authorize_launch, authority, ref, row)
        process = await asyncio.to_thread(subprocess.Popen, [sys.executable, '-c', 'import sys; sys.stdin.read()'],
                                          stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            birth = worker_birth(psutil.Process(process.pid))
            hello = {'type': 'hello', 'pid': process.pid, 'birth': birth, 'ancestors': [os.getpid()]}
            scope = await asyncio.to_thread(reserve_admission_worker, authority, admission_id=row['admission_id'],
                                            process=process, principal_id=row['principal_id'], hello=hello)
            raw = authority.db.get_meta('gateway.kanban.launch.v1:' + row['admission_id'])
            observed.append((json.loads(raw), scope['pid'], scope['birth']))
        finally:
            process.stdin.close()
            await asyncio.to_thread(process.wait, timeout=5)
        return ''

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    try:
        sid = (await run_task(owner.connection, owner.params))['session_id']
        await owner.authority._drain(SessionRef(owner.authority.profile_id, sid))
        assert len(observed) == 1
        record, pid, birth = observed[0]
        assert record['state'] == 'identified'
        assert (record['pid'], record['birth']) == (pid, birth)
        assert owner.authority.db.get_session(sid)['end_reason'] == 'kanban_attempt_terminal'
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_failed_attempt_finalizes_without_a_viewer(kanban_owner, monkeypatch):
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from gateway.run_shutdown_session_end import GatewaySessionEndMixin
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
    from hermes_state_runtime import list_session_admissions

    owner = kanban_owner(_CONFIG)
    authority = owner.authority
    manager = PluginManager()
    manager._discovered = True
    events = []
    PluginContext(PluginManifest(name='recorder', key='recorder'), manager).register_hook(
        'on_session_finalize', lambda **kw: events.append(kw))
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: manager)

    async def execute(*args):
        raise RuntimeError('synthetic admission execution failure')

    async def housekeeping(func, *args):
        return await asyncio.to_thread(func, *args)

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    runner = owner.runner
    runner._FINALIZE_TIMEOUT_S = 10
    runner._run_housekeeping_in_executor = housekeeping
    runner._run_release_in_profile_scope = lambda target, args, key: target(*args)
    runner._finalize_session_off_loop = MethodType(GatewaySessionEndMixin._finalize_session_off_loop, runner)
    try:
        sid = (await run_task(owner.connection, owner.params))['session_id']
        await owner.connection.close()
        await authority._drain(SessionRef(authority.profile_id, sid))
        [admission] = list_session_admissions(authority.db, session_id=sid, pending_only=False)
        assert admission['status'] == 'terminal' and admission['outcome'] == 'failed'
        assert authority.db.get_session(sid)['end_reason'] == 'kanban_attempt_terminal'
        assert len(events) == 1
        assert events[0]['session_id'] == sid
        assert events[0]['platform'] == 'cli'
        assert events[0]['reason'] == 'kanban_attempt_terminal'
        await authority._drain(SessionRef(authority.profile_id, sid))
        assert len(events) == 1
    finally:
        await owner.close()
