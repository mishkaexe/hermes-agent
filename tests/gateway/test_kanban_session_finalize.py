"""Finite Kanban sessions end at owner settlement, independently of their viewer."""
import asyncio
from types import MethodType

import pytest

from tests.gateway.test_kanban_finite_admission import _CONFIG, kanban_owner  # noqa: F401


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
