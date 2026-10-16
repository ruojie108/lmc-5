"""Serving JEV bridge for the exam endpoint (2026-10-16, her 14:51 ruling: the exam reranker is Jev).

Modeled on J50's offline JevBridge (round3/guchen_lmc5_test_20261009/jev_bridge.py) with two
deliberate serving-semantics changes, both to keep the exam endpoint ALIVE:
- no persistent HALT and no receipt cache: a failed/uncertain rerank falls back to the RRF order for
  THAT search only (meta mode "fallback"), the service keeps answering; overspend is still impossible
  because the budget ledger inside rerank_jev caps every send.
- no cross-request result cache: every Search reranks live (the offline digest cache could serve a
  stale ordering after the store changes under the same query).

The backend (rerank_jev) must be configured MODE=jev RETURN_N=100: the bridge hands it the full
100-candidate pool and cuts the output to min(top_k, 50) in every path, success or fallback.
"""
import sys
import time


class ExamJevBridge:
    ACTIVE = True
    MODE = "jev"
    RETURN_N = 50
    MAX_CAND = 100

    def __init__(self, backend, log=None):
        if backend.MODE != "jev" or backend.RETURN_N != 100:
            raise RuntimeError("jev_requires_full_pool_before_truncation")
        self.backend = backend
        self._log = log or (lambda rec: print("exam_jev_bridge %r" % (rec,), file=sys.stderr, flush=True))

    def stats_snapshot(self):
        return self.backend.stats_snapshot()

    def apply(self, query, cands, top_k, post_sort=None):
        n = min(int(top_k), self.RETURN_N)
        rrf_order = [(c[0], c[1]) for c in cands]  # the guaranteed fallback ordering, fixed up front
        try:
            pairs, meta = self.backend.apply(query, cands, 100)
            if meta.get("mode") == "fallback":
                # do not trust the backend's fallback pairs: restore the RRF order explicitly, and log
                self._log({"op": "jev_serving_fallback", "reason": str(meta.get("reason"))[:120],
                           "ts": round(time.time(), 3)})
                pairs = rrf_order
            elif meta.get("mode") not in ("rerank", "empty"):
                raise RuntimeError("jev_unknown_mode_%r" % (meta.get("mode"),))
        except Exception as exc:  # serving: degrade loudly, never halt the endpoint (process signals pass through)
            self._log({"op": "jev_serving_fallback", "error": type(exc).__name__, "ts": round(time.time(), 3)})
            pairs = rrf_order
            meta = {"mode": "fallback", "reason": "serving_degrade:" + type(exc).__name__}
        if post_sort is not None:
            pairs = post_sort(pairs)
        pairs = [tuple(p) for p in pairs][:n]
        return pairs, {**meta, "returned": len(pairs)}
