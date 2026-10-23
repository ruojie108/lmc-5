"""Search isolation and bounded scheduling without provider calls or a ledger."""
import importlib.util
from pathlib import Path
import threading
import time

import pytest

MODULE = Path(__file__).resolve().parents[1]


@pytest.fixture
def backend(monkeypatch, tmp_path):
    monkeypatch.setenv('AML_RERANK', 'off')
    monkeypatch.delenv('AML_RERANK_TEST_URL', raising=False)
    monkeypatch.delenv('AML_RETURN_N', raising=False)
    monkeypatch.setenv('AML_STATE_DIR', str(tmp_path))
    monkeypatch.delitem(__import__('sys').modules, 'aml_paths', raising=False)
    spec = importlib.util.spec_from_file_location('test_jev', MODULE / 'jev/rerank_jev.py')
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.MODE, m.RETURN_N = 'jev', 100
    m.PER_SEARCH, m.GLOBAL_INFLIGHT = 2, 2
    m.GLOBAL_SEM = threading.BoundedSemaphore(2)
    m.SCHED = m._Scheduler(100000, 200)
    m.DEADLINE, m.CALL_TIMEOUT = .2, .1
    m.BREAKER_FAILS, m.BREAKER_COOLDOWN = 3, .05
    m._token, m._token_loaded, m._token_state = 'stub-not-a-real-token', True, 'ok'
    yield m
    m._session.close()


def test_uncertain_call_does_not_abort_other_search(backend):
    b = backend
    both = threading.Barrier(2)
    calls, results = [], {}

    def call(model, token, query, memory, timeout, gate):
        gate.commit('synthetic')
        calls.append(query)
        both.wait(timeout=1)
        if query == 'failed':
            raise b.CallFail('net_timeout', sent='unknown', phase='transport')
        return b.Receipt(3., None, 200)

    b._call = call
    threads = [threading.Thread(target=lambda q=q: results.update({q: b.apply(q, [(1, .1, 'memory')], 1)}))
               for q in ['failed', 'healthy']]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2)
        assert not t.is_alive()
    assert results['failed'][1]['mode'] == 'fallback'
    assert results['healthy'][1]['mode'] == 'rerank'
    assert sorted(calls) == ['failed', 'healthy']


def test_no_send_after_search_deadline_even_if_worker_returns_late(backend):
    b = backend
    b.DEADLINE = .04
    calls = []
    finished = threading.Event()

    def delayed(model, token, query, memory, timeout, gate):
        try:
            time.sleep(.08)  # simulate slow DNS before any request byte
            gate.commit('after_dns')
            calls.append(query)
            return b.Receipt(3., None, 200)
        finally:
            finished.set()

    b._call = delayed
    out, meta = b.apply('question', [(1, .1, 'memory')], 1)
    assert out == [(1, .1)] and meta['mode'] == 'fallback'
    assert finished.wait(1)
    assert calls == []


def test_global_concurrency_and_stable_score_ties(backend):
    b = backend
    b.DEADLINE = 2
    lock = threading.Lock()
    active, high, calls = [0], [0], []

    def call(model, token, query, memory, timeout, gate):
        gate.commit('synthetic')
        with lock:
            active[0] += 1
            high[0] = max(high[0], active[0])
            calls.append((query, memory))
        time.sleep(.01)
        with lock:
            active[0] -= 1
        return b.Receipt(2., ('missing', None, None), 200)

    b._call = call
    cands = [(i, .1, str(i)) for i in range(8)]
    results = []
    threads = [threading.Thread(target=lambda q=q: results.append(b.apply(q, cands, 8))) for q in ['a', 'b']]
    for t in threads:
        t.start()
    for t in threads:
        t.join(3)
        assert not t.is_alive()
    assert high[0] <= 2 and len(calls) == len(set(calls)) == 16
    assert all(out == [(i, 2.) for i in range(8)] and meta['mode'] == 'rerank' for out, meta in results)


def test_breaker_cooldown_expires_without_manual_recovery(backend):
    b = backend

    def failure(model, token, query, memory, timeout, gate):
        gate.commit('synthetic')
        raise b.CallFail('http_503', sent='yes', phase='response', http_status=503)

    b._call = failure
    for _ in range(3):
        assert b.apply('q', [(1, .1, 'm')], 1)[1]['mode'] == 'fallback'
    assert b.apply('q', [(1, .1, 'm')], 1)[1]['reason'] == 'breaker_open'
    time.sleep(.06)
    b._call = lambda *a: b.Receipt(3., None, 200)
    assert b.apply('q', [(1, .1, 'm')], 1)[1]['mode'] == 'rerank'


def test_scheduler_rejects_expired_deadline_and_long_retry_after(backend):
    b = backend
    assert not b.SCHED.acquire(time.monotonic() - 1, threading.Event())
    b.SCHED.on_429(1000)
    assert not b.SCHED.acquire(time.monotonic() + 1, threading.Event())
    assert b.SCHED.hold_info()['paused_s'] > 999
