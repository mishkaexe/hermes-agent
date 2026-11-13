"""An owner keeps its physical cleanup holder until close really unwinds."""
import asyncio
import threading
from types import SimpleNamespace

import pytest

from gateway.session_contract import SessionRef


@pytest.mark.asyncio
async def test_cancelled_cleanup_keeps_worker_holder_until_close_returns(monkeypatch):
    from gateway import session_managed_worker as managed
    from tests.waiting import async_wait_until

    entered, release = threading.Event(), threading.Event()
    worker = SimpleNamespace(stop=threading.Event(), stop_cause=None)
    async def next_frame(*args, **kwargs):
        raise ValueError('no hello')
    def close():
        entered.set()
        release.wait(10)
    worker.next_frame = next_frame
    worker.close = close
    authority = SimpleNamespace(_managed_workers={}, _managed_spawning={},
        adopt_agent=lambda *args: None, pending_stops={}, pending_stop_causes={})
    ref = SessionRef('profile', 'session')
    row = {'admission_id': 'a', 'generation': 1, 'principal_id': 'p', 'payload': {'text': 'go'}}
    async def text(*args):
        return 'go', None
    async def spawn(*args):
        authority._managed_workers[ref.session_id] = worker
        return worker, None
    monkeypatch.setattr('gateway.session_context_refs.managed_turn_text', text)
    monkeypatch.setattr(managed, '_spawn_registered_worker', spawn)
    monkeypatch.setattr(managed, '_record_lost_attempt', lambda *args: None)
    task = asyncio.create_task(managed.execute_managed(authority, ref, row, None))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert authority._managed_workers.get(ref.session_id) is worker
    finally:
        release.set()
    await async_wait_until(lambda: ref.session_id not in authority._managed_workers,
                           timeout=5, what='cleanup holder to release')
