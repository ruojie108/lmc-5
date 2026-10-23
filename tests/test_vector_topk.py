"""Streaming recall must preserve full-scan ranking while hydrating only winners."""
import random

import pytest

from lmc5.store import MemoryStore


@pytest.fixture
def vectors(tmp_path):
    rng = random.Random(42)
    with MemoryStore(tmp_path / 'vectors.sqlite') as store:
        store.init()
        for i in range(220):
            record, _ = store.add_memory(title=f'Synthetic {i}', content=f'Synthetic memory {i}')
            # Include zero vectors, ties and a best match beyond the first 100 rows.
            vector = [rng.uniform(-1, 1) for _ in range(8)]
            if i % 15 == 0:
                vector = [0.] * 8
            if i in (201, 202):
                vector = [1., .0004 if i == 201 else .0003] + [0.] * 6
            store.upsert_vector(owner_type='memory', owner_id=record.id, vector=vector,
                                provider='synthetic', model='small')
        event, _ = store.log_event(role='user', content='Synthetic event')
        store.upsert_vector(owner_type='event', owner_id=event.id, vector=[1.] + [0.] * 7,
                            provider='synthetic', model='small')
        yield store


@pytest.mark.parametrize('owner', ['memory', 'event', None])
@pytest.mark.parametrize('limit', [0, 1, 10, 100, 300, -1])
@pytest.mark.parametrize('redact', [True, False])
def test_matches_original_full_scan(vectors, owner, limit, redact):
    args = dict(query_vector=[1.] + [0.] * 7, provider='synthetic', model='small',
                owner_type=owner, limit=limit, redact=redact)
    assert vectors.search_vectors(**args) == vectors._search_vectors_full(**args)


def test_scans_beyond_first_100_and_preserves_rounded_ties(vectors):
    hits = vectors.search_vectors(query_vector=[1.] + [0.] * 7, provider='synthetic',
                                  model='small', owner_type='memory', limit=100)
    assert len(hits) == 100
    assert [x['record']['title'] for x in hits[:2]] == ['Synthetic 201', 'Synthetic 202']
    assert hits[0]['score'] == hits[1]['score'] == 1.0


def test_only_selected_records_are_hydrated(vectors, monkeypatch):
    get = vectors.get_memory
    hydrated = []

    def counted(mid):
        hydrated.append(mid)
        return get(mid)

    monkeypatch.setattr(vectors, 'get_memory', counted)
    result = vectors.search_vectors(query_vector=[1.] + [0.] * 7, provider='synthetic',
                                    model='small', owner_type='memory', limit=100)
    assert len(hydrated) == 100
    assert hydrated == [hit['owner_id'] for hit in result]


def test_dangling_low_rank_vector_still_raises(vectors):
    vectors.conn.execute("UPDATE vectors SET owner_id=999999 WHERE owner_type='memory' AND id=1")
    args = dict(query_vector=[1.] + [0.] * 7, provider='synthetic', model='small', limit=1)
    for method in (vectors.search_vectors, vectors._search_vectors_full):
        with pytest.raises(KeyError, match='memory not found: 999999'):
            method(**args)


def test_absent_model_has_no_hits(vectors):
    assert vectors.search_vectors(query_vector=[1.] + [0.] * 7, provider='synthetic',
                                  model='missing', limit=100) == []
