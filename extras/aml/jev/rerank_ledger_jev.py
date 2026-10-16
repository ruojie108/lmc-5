"""Transactional budget ledger for the WO2 rerank hook (revision 3, 2026-10-03; rev2 base).

Revision 3 (review WO123_latest_free_review_chenchen.json, WO2_v2 W2-A/B/D):
- W2-B: financial invariants (charge == SUM(adjustments) per attempt, global exposure == SUM(all
  adjustments), no orphan adjustments, status/charge rules, runtime SETTLED == tokens x price,
  unresolved holds have an open stop, exposure <= grants, known stop kinds) are checked by validate()
  at process start, by recover_orphans() inside its own transaction, and by resolve_attempt() and
  resume() INSIDE the same BEGIN IMMEDIATE transaction that would clear a stop. Any problem ->
  validate raises ledger_audit_failed, resume returns resumed=False, resolve raises. quick_check /
  integrity_check alone is never taken as financial consistency.
- W2-A: verify_settlement() lets the caller prove (read-only) that an attempt already carries this
  exact settlement (same attempt_id + run_id + status + charge + usage receipt + http status).
- W2-D: route_state / route_events tables (schema 3). finalize() takes a route observation and, in
  the SAME transaction as the settlement: 401/403 -> AUTH_REJECTED stop on first occurrence; any
  other 4xx (incl. 429) -> consecutive counter +1 in observation (commit) order, 3 -> HTTP_4XX_STREAK
  stop; a valid 200 (score AND usage valid) -> counter 0; 429 -> pause_until (wall clock epoch) from
  the original Retry-After. mark_sending() refuses while the persisted pause is active. Counter and
  pause survive restarts; only an audited resume resets the counter (the pause is never cleared).
  Schema-2 ledgers are refused until an explicit, authorised migrate (wo2_budget_admin.py migrate).

SQLite, rollback journal, synchronous=FULL, every mutation in one BEGIN IMMEDIATE transaction
(atomic + durable on commit; a crash mid-write leaves a hot journal that SQLite rolls back).
Money is integer nano-USD (1e-9 USD). No floats are stored.

Hard rules:
- The ledger file is created ONLY by an explicit, authorised init (wo2_budget_admin.py init).
  A missing / empty / corrupt / foreign ledger is refused; nothing is ever zero-initialised.
- One row per attempt (candidate call): identity, reservation, send state, raw usage (numeric
  metadata only), final charge. Every change of an attempt's charge is an adjustments row, so
  charge == SUM(adjustments) is an auditable invariant.
- Sticky stops live in the stops table. Any unresolved stop blocks every new reservation AND
  every new send (checked inside the same transaction that marks an attempt SENDING).
  Stops are resolved only through the audited admin path, never by flipping a flag.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import sqlite3
import time
import uuid
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

SCHEMA_VERSION = "3"
NANO = 10 ** 9

# attempt statuses
OPEN = ("RESERVED", "SENDING")                       # charge == est, still owned by a live run
HOLD_UNRESOLVED = ("UNKNOWN", "USAGE_INVALID", "HISTORY_UNKNOWN")   # charge == est, line stopped
RELEASED = ("RELEASED_NOT_SENT", "RELEASED_NOT_SENT_PROVEN", "RELEASED_HTTP_4XX",
            "RELEASED_AFTER_REVIEW")                 # charge == 0
SETTLED = ("SETTLED", "SETTLED_OVER", "SETTLED_AFTER_REVIEW", "HISTORY_SETTLED", "EXTERNAL_ESTIMATE")
KEPT = ("UNKNOWN_KEPT", "USAGE_INVALID_KEPT")        # charge == est, resolved by admin
ALL_STATUSES = OPEN + HOLD_UNRESOLVED + RELEASED + SETTLED + KEPT
STOP_KINDS = ("BUDGET_INSUFFICIENT", "UNKNOWN", "USAGE_INVALID", "OVER_RESERVATION", "HISTORY_UNKNOWN",
              "LEDGER_WRITE_FAILED", "AUTH_REJECTED", "HTTP_4XX_STREAK")
SOFT_STOPS = ("BUDGET_INSUFFICIENT",)     # the ONLY stop that still lets an already-reserved slot send
HTTP_4XX_STREAK_N = 3
RUNTIME_SETTLED = ("SETTLED", "SETTLED_OVER")
MODEL_PRICE_NANO = {"clef": 240, "clef-flash": 90, "jev-1.13.0": 42}   # same integers as rerank.MODELS (J50: + jev, ruling F1-a)

SCHEMA = """
CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE grants(id INTEGER PRIMARY KEY, ts TEXT NOT NULL, nano INTEGER NOT NULL CHECK(nano > 0),
                    actor TEXT NOT NULL, note TEXT NOT NULL);
CREATE TABLE runs(run_id TEXT PRIMARY KEY, ts_start TEXT NOT NULL, pid INTEGER, host TEXT, mode TEXT,
                  model TEXT, route TEXT, env_cap_nano INTEGER, test_mode INTEGER);
CREATE TABLE attempts(
  attempt_id TEXT PRIMARY KEY, search_id TEXT, run_id TEXT, slot INTEGER, model TEXT,
  est_nano INTEGER NOT NULL CHECK(est_nano >= 0), charge_nano INTEGER NOT NULL CHECK(charge_nano >= 0),
  status TEXT NOT NULL, sent TEXT, phase TEXT, err TEXT, err_errno INTEGER, http_status INTEGER,
  usage_status TEXT, usage_raw TEXT, usage_tokens INTEGER,
  ts_reserved TEXT, ts_sending TEXT, ts_final TEXT, note TEXT);
CREATE INDEX attempts_status ON attempts(status);
CREATE INDEX attempts_search ON attempts(search_id);
CREATE TABLE adjustments(id INTEGER PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
  ts TEXT NOT NULL, delta_nano INTEGER NOT NULL, kind TEXT NOT NULL, actor TEXT NOT NULL, note TEXT);
CREATE INDEX adjustments_attempt ON adjustments(attempt_id);
CREATE TABLE stops(id INTEGER PRIMARY KEY, ts TEXT NOT NULL, kind TEXT NOT NULL, attempt_id TEXT, run_id TEXT,
  note TEXT, resolved_ts TEXT, resolved_by TEXT, resolve_note TEXT);
"""
SCHEMA_V3_ADD = """
CREATE TABLE route_state(route TEXT PRIMARY KEY, consec_4xx INTEGER NOT NULL DEFAULT 0 CHECK(consec_4xx >= 0),
  pause_until_epoch REAL, pause_reason TEXT, last_event_seq INTEGER NOT NULL DEFAULT 0, updated_ts TEXT);
CREATE TABLE route_events(seq INTEGER PRIMARY KEY AUTOINCREMENT, route TEXT NOT NULL, ts TEXT NOT NULL,
  attempt_id TEXT, run_id TEXT, http_status INTEGER, kind TEXT NOT NULL, consec_after INTEGER,
  retry_after_raw TEXT, pause_until_epoch REAL);
"""


class LedgerError(Exception):
    """code is a short metadata string, e.g. ledger_missing / ledger_empty / ledger_corrupt."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def now_iso() -> str:
    t = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + ".%03dZ" % int((t % 1) * 1000)


def nano_to_usd(n: int) -> str:
    return format((Decimal(int(n)) / NANO).quantize(Decimal("0.000000001")), "f")


_DEC_RE = re.compile(r"[0-9]{1,9}(?:\.[0-9]{1,12})?")


def usd_to_nano(s, *, ceil: bool = False) -> int:
    """Strict: plain non-negative decimal text only (no nan/inf/exponent/sign)."""
    if isinstance(s, bool) or not isinstance(s, str) or not _DEC_RE.fullmatch(s.strip()):
        raise ValueError("amount must be a plain decimal string")
    d = Decimal(s.strip())
    if not d.is_finite() or d < 0:
        raise ValueError("amount must be finite and >= 0")
    return int((d * NANO).to_integral_value(ROUND_CEILING if ceil else ROUND_FLOOR))


def _fsync_dir(p: Path) -> None:
    try:
        fd = os.open(str(p), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


class Ledger:
    def __init__(self, path):
        self.path = Path(path)

    # -- plumbing ---------------------------------------------------------------------------------
    def _connect(self, timeout: float) -> sqlite3.Connection:
        try:
            st = self.path.stat()
        except FileNotFoundError:
            raise LedgerError("ledger_missing")
        if st.st_size == 0:
            raise LedgerError("ledger_empty")
        try:
            c = sqlite3.connect(self.path.resolve().as_uri() + "?mode=rw", uri=True,
                                timeout=max(0.01, float(timeout)), isolation_level=None)
            c.execute("PRAGMA synchronous=FULL")
            c.execute("PRAGMA foreign_keys=ON")
            return c
        except sqlite3.DatabaseError as e:
            raise LedgerError("ledger_open_" + type(e).__name__)

    def tx(self, fn, timeout: float, write: bool = True):
        c = self._connect(timeout)
        try:
            try:
                c.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            except sqlite3.OperationalError as e:
                raise LedgerError("ledger_busy" if "lock" in str(e) else "ledger_corrupt")
            except sqlite3.DatabaseError:
                raise LedgerError("ledger_corrupt")
            try:
                out = fn(c)
            except BaseException:
                try:
                    c.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            try:
                c.execute("COMMIT")
            except sqlite3.Error as e:
                try:
                    c.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise LedgerError("ledger_commit_" + type(e).__name__)
            return out
        except sqlite3.DatabaseError as e:
            raise LedgerError("ledger_db_" + type(e).__name__)
        finally:
            c.close()

    # -- creation (admin only) --------------------------------------------------------------------
    @classmethod
    def init_new(cls, path, *, budget_id: str, route: str, grant_nano: int, actor: str, note: str) -> "Ledger":
        p = Path(path)
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,80}", budget_id or ""):
            raise LedgerError("bad_budget_id")
        if grant_nano <= 0:
            raise LedgerError("grant_must_be_positive")
        p.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(str(p), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)   # never reuse/overwrite
        except FileExistsError:
            raise LedgerError("ledger_exists_refuse_init")
        os.close(fd)
        canon = str(p.resolve())
        c = sqlite3.connect(p.resolve().as_uri() + "?mode=rw", uri=True, isolation_level=None)
        try:
            c.execute("PRAGMA journal_mode=DELETE")
            c.execute("PRAGMA synchronous=FULL")
            c.execute("BEGIN IMMEDIATE")
            for stmt in [s for s in (SCHEMA + SCHEMA_V3_ADD).split(";") if s.strip()]:
                c.execute(stmt)
            ts = now_iso()
            c.execute("INSERT INTO route_state(route, consec_4xx, updated_ts) VALUES(?, 0, ?)", (route, ts))
            for k, v in (("schema_version", SCHEMA_VERSION), ("budget_id", budget_id), ("route", route),
                         ("canonical_path", canon), ("created", ts), ("created_by", actor),
                         ("ledger_uuid", uuid.uuid4().hex)):
                c.execute("INSERT INTO meta(k, v) VALUES(?, ?)", (k, v))
            c.execute("INSERT INTO grants(ts, nano, actor, note) VALUES(?,?,?,?)", (ts, int(grant_nano), actor, note))
            c.execute("COMMIT")
        finally:
            c.close()
        _fsync_dir(p.parent)
        return cls(p)

    # -- validation at process start --------------------------------------------------------------
    def validate(self, *, budget_id: str, route: str, timeout: float = 5.0) -> dict:
        def f(c):
            try:
                qc = c.execute("PRAGMA quick_check").fetchone()[0]
            except sqlite3.DatabaseError:
                raise LedgerError("ledger_corrupt")
            if qc != "ok":
                raise LedgerError("ledger_corrupt")
            names = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {"meta", "grants", "runs", "attempts", "adjustments", "stops"} <= names:
                raise LedgerError("ledger_schema_missing")
            meta = dict(c.execute("SELECT k, v FROM meta"))
            if meta.get("schema_version") == "2":
                raise LedgerError("ledger_schema_v2_needs_migrate")
            if meta.get("schema_version") != SCHEMA_VERSION:
                raise LedgerError("ledger_schema_version")
            if not {"route_state", "route_events"} <= names:
                raise LedgerError("ledger_schema_missing")
            if meta.get("budget_id") != budget_id:
                raise LedgerError("ledger_budget_id_mismatch")
            if meta.get("route") != route:
                raise LedgerError("ledger_route_mismatch")
            if meta.get("canonical_path") != str(self.path.resolve()):
                raise LedgerError("ledger_moved_or_copied")
            if not c.execute("SELECT COUNT(*) FROM grants").fetchone()[0]:
                raise LedgerError("ledger_no_grant")
            if not c.execute("SELECT 1 FROM route_state WHERE route=?", (route,)).fetchone():
                raise LedgerError("ledger_route_state_missing")
            probs = self._financial_problems(c)       # W2-B: quick_check is NOT financial consistency
            if probs:
                raise LedgerError("ledger_audit_failed")
            return meta
        return self.tx(f, timeout, write=False)

    # -- helpers inside a transaction -------------------------------------------------------------
    @staticmethod
    def _cap(c) -> int:
        return int(c.execute("SELECT COALESCE(SUM(nano), 0) FROM grants").fetchone()[0])

    @staticmethod
    def _exposure(c) -> int:
        return int(c.execute("SELECT COALESCE(SUM(charge_nano), 0) FROM attempts").fetchone()[0])

    @staticmethod
    def _open_stops(c) -> list:
        return [r[0] for r in c.execute("SELECT kind FROM stops WHERE resolved_ts IS NULL ORDER BY id")]

    @staticmethod
    def _financial_problems(c) -> list:
        """Logical/financial invariants (W2-B). Pure reads inside the caller's transaction."""
        problems = []
        adj = {r[0]: int(r[1]) for r in c.execute("SELECT attempt_id, COALESCE(SUM(delta_nano),0) FROM adjustments "
                                                   "GROUP BY attempt_id")}
        known_runs = {r[0] for r in c.execute("SELECT run_id FROM runs")}
        ids = set()
        for aid, st, est, ch, run, model, ustat, utok in c.execute(
                "SELECT attempt_id, status, est_nano, charge_nano, run_id, model, usage_status, usage_tokens FROM attempts"):
            ids.add(aid)
            if adj.get(aid, 0) != int(ch):
                problems.append("charge_ne_adjustments:" + aid)
            if st not in ALL_STATUSES:
                problems.append("bad_status:" + aid)
            if int(ch) < 0 or int(est) < 0:
                problems.append("negative_amount:" + aid)
            if st in RELEASED and ch != 0:
                problems.append("released_nonzero:" + aid)
            if st in OPEN + HOLD_UNRESOLVED + KEPT and ch != est:
                problems.append("hold_ne_est:" + aid)
            if st in RUNTIME_SETTLED:
                price = MODEL_PRICE_NANO.get(model)
                if ustat != "ok" or utok is None or price is None or int(utok) * price != int(ch):
                    problems.append("settled_ne_tokens_x_price:" + aid)
            if run != "history" and run not in known_runs:
                problems.append("attempt_run_unregistered:" + aid)
        for aid in adj:
            if aid not in ids:
                problems.append("adjustment_without_attempt:" + str(aid))
        exp = int(c.execute("SELECT COALESCE(SUM(charge_nano),0) FROM attempts").fetchone()[0])
        tot = int(c.execute("SELECT COALESCE(SUM(delta_nano),0) FROM adjustments").fetchone()[0])
        if exp != tot:
            problems.append("exposure_ne_sum_adjustments:%d!=%d" % (exp, tot))
        grants = int(c.execute("SELECT COALESCE(SUM(nano),0) FROM grants").fetchone()[0])
        if c.execute("SELECT COUNT(*) FROM grants WHERE nano <= 0").fetchone()[0]:
            problems.append("nonpositive_grant")
        if exp > grants:
            problems.append("exposure_exceeds_grants:%d>%d" % (exp, grants))
        for aid, in c.execute("SELECT attempt_id FROM attempts WHERE status IN (%s)"
                              % ",".join("?" * len(HOLD_UNRESOLVED)), HOLD_UNRESOLVED).fetchall():
            if not c.execute("SELECT 1 FROM stops WHERE attempt_id=? AND resolved_ts IS NULL", (aid,)).fetchone():
                problems.append("unresolved_without_open_stop:" + aid)
        for k, in c.execute("SELECT DISTINCT kind FROM stops").fetchall():
            if k not in STOP_KINDS:
                problems.append("unknown_stop_kind:" + str(k))
        try:
            for (n,) in c.execute("SELECT consec_4xx FROM route_state").fetchall():
                if n is None or int(n) < 0:
                    problems.append("route_state_bad_counter")
        except sqlite3.OperationalError:
            pass                                   # schema 2 (no route tables) handled by validate
        return problems

    @staticmethod
    def _adjust(c, attempt_id, delta, kind, actor, note=None):
        c.execute("INSERT INTO adjustments(attempt_id, ts, delta_nano, kind, actor, note) VALUES(?,?,?,?,?,?)",
                  (attempt_id, now_iso(), int(delta), kind, actor, note))

    @staticmethod
    def _stop(c, kind, attempt_id=None, run_id=None, note=None):
        c.execute("INSERT INTO stops(ts, kind, attempt_id, run_id, note) VALUES(?,?,?,?,?)",
                  (now_iso(), kind, attempt_id, run_id, note))

    # -- runtime operations -----------------------------------------------------------------------
    def register_run(self, *, run_id, mode, model, route, env_cap_nano, test_mode, timeout=5.0):
        def f(c):
            if c.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone():
                raise LedgerError("run_id_reused")
            c.execute("INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?)",
                      (run_id, now_iso(), os.getpid(), socket.gethostname(), mode, model, route,
                       int(env_cap_nano), int(bool(test_mode))))
        self.tx(f, timeout)

    def recover_orphans(self, current_run_id, actor, timeout=5.0) -> dict:
        """Caller must hold the instance lock, so OPEN attempts of other runs are orphans.
        RESERVED orphan: SENDING is committed BEFORE any byte is handed to HTTP, so a RESERVED row
        was provably never sent -> RELEASED_NOT_SENT_PROVEN. SENDING orphan -> UNKNOWN + stop."""
        def f(c):
            out = {"reserved_released": 0, "sending_unknown": 0}
            for aid, st, est, ch in c.execute("SELECT attempt_id, status, est_nano, charge_nano FROM attempts "
                                              "WHERE status IN ('RESERVED','SENDING') AND run_id != ?",
                                              (current_run_id,)).fetchall():
                if st == "RESERVED":
                    c.execute("UPDATE attempts SET status='RELEASED_NOT_SENT_PROVEN', charge_nano=0, sent='no', "
                              "ts_final=?, note='orphan_reserved_never_marked_sending' WHERE attempt_id=?",
                              (now_iso(), aid))
                    self._adjust(c, aid, -ch, "release_orphan_reserved", actor)
                    out["reserved_released"] += 1
                else:
                    c.execute("UPDATE attempts SET status='UNKNOWN', sent='unknown', ts_final=?, "
                              "note='orphan_sending_process_gone' WHERE attempt_id=?", (now_iso(), aid))
                    self._stop(c, "UNKNOWN", aid, current_run_id, "orphan_sending_process_gone")
                    out["sending_unknown"] += 1
            if self._financial_problems(c):          # W2-B: same transaction, rolled back on failure
                raise LedgerError("ledger_audit_failed")
            return out
        return self.tx(f, timeout)

    def reserve_search(self, *, search_id, run_id, model, ests, env_cap_nano, timeout):
        """One transaction: check stops, check cap (min(grants, env cap)), insert one RESERVED row
        per candidate. Returns ("ok", [attempt_ids]) | ("stopped", kinds) | ("insufficient", None)."""
        def f(c):
            stops = self._open_stops(c)
            if stops:
                return "stopped", stops
            cap = min(self._cap(c), int(env_cap_nano))
            need = sum(int(e) for e in ests)
            if self._exposure(c) + need > cap:
                self._stop(c, "BUDGET_INSUFFICIENT", None, run_id,
                           "need=%d exposure=%d cap=%d" % (need, self._exposure(c), cap))
                return "insufficient", None
            ids, ts = [], now_iso()
            for slot, e in enumerate(ests):
                aid = uuid.uuid4().hex
                c.execute("INSERT INTO attempts(attempt_id, search_id, run_id, slot, model, est_nano, charge_nano, "
                          "status, ts_reserved) VALUES(?,?,?,?,?,?,?,'RESERVED',?)",
                          (aid, search_id, run_id, slot, model, int(e), int(e), ts))
                self._adjust(c, aid, int(e), "reserve", run_id)
                ids.append(aid)
            return "ok", ids
        return self.tx(f, timeout)

    def mark_sending(self, attempt_id, run_id, timeout) -> str:
        """Committed BEFORE the HTTP layer sees the request. Refused if any hard stop is open
        (UNKNOWN / USAGE_INVALID / OVER_RESERVATION / HISTORY_UNKNOWN / LEDGER_WRITE_FAILED).
        BUDGET_INSUFFICIENT blocks new reservations only: money already reserved is within cap."""
        def f(c):
            if [k for k in self._open_stops(c) if k not in SOFT_STOPS]:
                return "stopped"
            pu = c.execute("SELECT MAX(pause_until_epoch) FROM route_state").fetchone()[0]
            if pu is not None and float(pu) > time.time():
                return "paused"                          # W2-D: persisted Retry-After hold
            n = c.execute("UPDATE attempts SET status='SENDING', ts_sending=? WHERE attempt_id=? AND run_id=? "
                          "AND status='RESERVED'", (now_iso(), attempt_id, run_id)).rowcount
            return "ok" if n == 1 else "bad_state"
        return self.tx(f, timeout)

    def release_unsent(self, attempt_ids, run_id, reason, timeout) -> int:
        """Only RESERVED rows (never marked SENDING => never handed to HTTP)."""
        def f(c):
            k = 0
            for aid in attempt_ids:
                row = c.execute("SELECT charge_nano FROM attempts WHERE attempt_id=? AND run_id=? AND status='RESERVED'",
                                (aid, run_id)).fetchone()
                if not row:
                    continue
                c.execute("UPDATE attempts SET status='RELEASED_NOT_SENT', charge_nano=0, sent='no', ts_final=?, "
                          "note=? WHERE attempt_id=?", (now_iso(), reason, aid))
                self._adjust(c, aid, -int(row[0]), "release_not_sent", run_id, reason)
                k += 1
            return k
        return self.tx(f, timeout)

    def finalize(self, attempt_id, run_id, *, status, charge_nano, fields: dict, stop_kind=None, route_obs=None,
                 result=None, timeout=10.0) -> bool:
        """Exactly-once settlement: only a SENDING row of this run can be finalised. A second call
        for the same attempt returns False and changes nothing (no double settlement).
        route_obs (W2-D): {"kind": "auth"|"4xx"|"ok200"|None, "http_status", "retry_after_raw",
        "pause_until_epoch"} applied in the same transaction. result (dict) receives stops_added/consec."""
        assert status in ALL_STATUSES and status not in OPEN
        cols = ("sent", "phase", "err", "err_errno", "http_status", "usage_status", "usage_raw", "usage_tokens", "note")

        def f(c):
            row = c.execute("SELECT charge_nano FROM attempts WHERE attempt_id=? AND run_id=? AND status='SENDING'",
                            (attempt_id, run_id)).fetchone()
            if not row:
                return False
            vals = [fields.get(k) for k in cols]
            c.execute("UPDATE attempts SET status=?, charge_nano=?, ts_final=?, " + ", ".join(k + "=?" for k in cols)
                      + " WHERE attempt_id=?", [status, int(charge_nano), now_iso()] + vals + [attempt_id])
            self._adjust(c, attempt_id, int(charge_nano) - int(row[0]), "finalize_" + status.lower(), run_id)
            added = []
            if stop_kind:
                self._stop(c, stop_kind, attempt_id, run_id, fields.get("err") or fields.get("usage_status"))
                added.append(stop_kind)
            consec = None
            if route_obs and route_obs.get("kind"):
                added += self._route_observe(c, attempt_id, run_id, route_obs)
                consec = c.execute("SELECT MAX(consec_4xx) FROM route_state").fetchone()[0]
            if result is not None:
                result["stops_added"] = added
                result["consec_4xx"] = consec
            return True
        return self.tx(f, timeout)

    def _route_observe(self, c, attempt_id, run_id, obs) -> list:
        route = dict(c.execute("SELECT k, v FROM meta")).get("route")
        row = c.execute("SELECT consec_4xx, pause_until_epoch FROM route_state WHERE route=?", (route,)).fetchone()
        if row is None:
            raise LedgerError("ledger_route_state_missing")
        consec, pu = int(row[0]), row[1]
        kind, hs, added = obs["kind"], obs.get("http_status"), []
        if kind == "auth":
            self._stop(c, "AUTH_REJECTED", attempt_id, run_id, "http_%s" % hs)
            added.append("AUTH_REJECTED")
        elif kind == "4xx":
            consec += 1
            if consec >= HTTP_4XX_STREAK_N and "HTTP_4XX_STREAK" not in self._open_stops(c):
                self._stop(c, "HTTP_4XX_STREAK", attempt_id, run_id, "consecutive_4xx=%d last=http_%s" % (consec, hs))
                added.append("HTTP_4XX_STREAK")
        elif kind == "ok200":
            consec = 0
        npu = obs.get("pause_until_epoch")
        if npu is not None:
            pu = max(float(pu or 0.0), float(npu))
        c.execute("INSERT INTO route_events(route, ts, attempt_id, run_id, http_status, kind, consec_after, retry_after_raw, "
                  "pause_until_epoch) VALUES(?,?,?,?,?,?,?,?,?)",
                  (route, now_iso(), attempt_id, run_id, hs, kind, consec, obs.get("retry_after_raw"), npu))
        seq = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.execute("UPDATE route_state SET consec_4xx=?, pause_until_epoch=?, "
                  "pause_reason=CASE WHEN ? IS NULL THEN pause_reason ELSE 'http_429' END, last_event_seq=?, updated_ts=? "
                  "WHERE route=?", (consec, pu, npu, seq, now_iso(), route))
        return added

    def route_state(self, timeout=2.0) -> dict:
        def f(c):
            r = c.execute("SELECT route, consec_4xx, pause_until_epoch, pause_reason, last_event_seq FROM route_state").fetchone()
            if r is None:
                raise LedgerError("ledger_route_state_missing")
            return {"route": r[0], "consec_4xx": int(r[1]), "pause_until_epoch": r[2], "pause_reason": r[3],
                    "last_event_seq": int(r[4]),
                    "pause_remaining_s": max(0.0, float(r[2]) - time.time()) if r[2] is not None else 0.0}
        return self.tx(f, timeout, write=False)

    def get_attempt(self, attempt_id, timeout=2.0):
        def f(c):
            c.row_factory = sqlite3.Row
            r = c.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            return dict(r) if r else None
        return self.tx(f, timeout, write=False)

    def verify_settlement(self, attempt_id, run_id, *, status, charge_nano, usage_tokens, usage_status, http_status,
                          timeout=2.0) -> bool:
        """W2-A: independent read-only proof that THIS settlement is already durable (same identity,
        same receipt). Used when finalize() raised or returned False."""
        try:
            r = self.get_attempt(attempt_id, timeout=timeout)
        except LedgerError:
            return False
        return bool(r) and r["run_id"] == run_id and r["status"] == status and int(r["charge_nano"]) == int(charge_nano) \
            and r["usage_tokens"] == usage_tokens and r["usage_status"] == usage_status and r["http_status"] == http_status \
            and r["ts_final"] is not None

    def add_stop(self, kind, run_id, note, timeout=5.0, attempt_id=None):
        if kind not in STOP_KINDS:
            raise LedgerError("bad_stop_kind")
        self.tx(lambda c: self._stop(c, kind, attempt_id, run_id, note), timeout)

    def reconcile_anomalies(self, entries, run_id, timeout=5.0) -> int:
        """W2-A recovery: an anomaly-journal entry whose attempt never got ANY stop row in the ledger
        gets a LEDGER_WRITE_FAILED stop now. Returns the number of stops added."""
        def f(c):
            k = 0
            for e in entries:
                aid = e.get("attempt_id")
                if not aid or c.execute("SELECT 1 FROM stops WHERE attempt_id=?", (aid,)).fetchone():
                    continue
                self._stop(c, "LEDGER_WRITE_FAILED", aid, run_id, "anomaly_journal:" + str(e.get("why"))[:60])
                k += 1
            return k
        return self.tx(f, timeout)

    def snapshot(self, env_cap_nano=None, timeout=1.0) -> dict:
        def f(c):
            grants = self._cap(c)
            cap = grants if env_cap_nano is None else min(grants, int(env_cap_nano))
            stops = self._open_stops(c)
            by = {r[0]: [int(r[1]), int(r[2])] for r in c.execute(
                "SELECT status, COUNT(*), COALESCE(SUM(charge_nano),0) FROM attempts GROUP BY status")}
            exp = self._exposure(c)
            return {"grants_usd": nano_to_usd(grants), "cap_usd": nano_to_usd(cap),
                    "committed_usd": nano_to_usd(exp), "available_usd": nano_to_usd(max(0, cap - exp)),
                    "committed_nano": exp, "cap_nano": cap,
                    "stopped": bool(stops), "open_stops": stops,
                    "unresolved_unknown": sum(by.get(s, [0, 0])[0] for s in HOLD_UNRESOLVED),
                    "open_attempts": sum(by.get(s, [0, 0])[0] for s in OPEN),
                    "by_status": by}
        return self.tx(f, timeout, write=False)

    # -- audit / admin ----------------------------------------------------------------------------
    def audit(self, timeout=10.0) -> dict:
        def f(c):
            problems = []
            ic = c.execute("PRAGMA integrity_check").fetchone()[0]
            if ic != "ok":
                problems.append("integrity_check:" + ic[:80])
            problems += self._financial_problems(c)
            unresolved = [r[0] for r in c.execute("SELECT attempt_id FROM attempts WHERE status IN (%s)"
                                                  % ",".join("?" * len(HOLD_UNRESOLVED)), HOLD_UNRESOLVED)]
            return {"ok": not problems, "problems": problems[:50], "n_problems": len(problems),
                    "unresolved_attempts": unresolved,
                    "open_attempts": [r[0] for r in c.execute("SELECT attempt_id FROM attempts WHERE status IN ('RESERVED','SENDING')")]}
        return self.tx(f, timeout, write=False)

    def add_grant(self, nano, actor, note, timeout=5.0):
        if nano <= 0:
            raise LedgerError("grant_must_be_positive")
        self.tx(lambda c: c.execute("INSERT INTO grants(ts, nano, actor, note) VALUES(?,?,?,?)",
                                    (now_iso(), int(nano), actor, note)), timeout)

    def import_history(self, *, status, nano, tokens, model, actor, note, timeout=5.0) -> str:
        if status not in ("HISTORY_SETTLED", "HISTORY_UNKNOWN", "EXTERNAL_ESTIMATE"):
            raise LedgerError("bad_history_status")

        def f(c):
            aid = "hist-" + uuid.uuid4().hex[:24]
            c.execute("INSERT INTO attempts(attempt_id, search_id, run_id, slot, model, est_nano, charge_nano, status, "
                      "sent, usage_tokens, ts_reserved, ts_final, note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (aid, None, "history", None, model, int(nano), int(nano), status,
                       "unknown" if status == "HISTORY_UNKNOWN" else "yes", tokens, now_iso(), now_iso(), note))
            self._adjust(c, aid, int(nano), "import_" + status.lower(), actor, note)
            if status == "HISTORY_UNKNOWN":
                self._stop(c, "HISTORY_UNKNOWN", aid, None, note)
            return aid
        return self.tx(f, timeout)

    def resolve_attempt(self, attempt_id, *, action, actor, evidence, tokens=None, price_nano_per_token=None,
                        timeout=5.0) -> dict:
        """action: keep (charge stays = est, resolved) | release (charge -> 0, needs not-sent evidence)
        | settle (charge -> tokens*price from a verified receipt). Only for unresolved holds."""
        if not evidence or len(evidence.strip()) < 8:
            raise LedgerError("evidence_required")

        def f(c):
            row = c.execute("SELECT status, est_nano, charge_nano FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if not row or row[0] not in HOLD_UNRESOLVED:
                raise LedgerError("not_an_unresolved_hold")
            st, est, ch = row
            if action == "keep":
                new_st, new_ch = ("USAGE_INVALID_KEPT" if st == "USAGE_INVALID" else "UNKNOWN_KEPT"), ch
            elif action == "release":
                new_st, new_ch = "RELEASED_AFTER_REVIEW", 0
            elif action == "settle":
                if tokens is None or price_nano_per_token is None or tokens < 0:
                    raise LedgerError("settle_needs_tokens_and_price")
                new_st, new_ch = "SETTLED_AFTER_REVIEW", int(tokens) * int(price_nano_per_token)
            else:
                raise LedgerError("bad_action")
            c.execute("UPDATE attempts SET status=?, charge_nano=?, ts_final=?, note=COALESCE(note,'') || ? "
                      "WHERE attempt_id=?", (new_st, new_ch, now_iso(), " | review:" + action, attempt_id))
            self._adjust(c, attempt_id, new_ch - ch, "review_" + action, actor, evidence)
            c.execute("UPDATE stops SET resolved_ts=?, resolved_by=?, resolve_note=? WHERE attempt_id=? AND resolved_ts IS NULL",
                      (now_iso(), actor, action + ": " + evidence, attempt_id))
            if self._financial_problems(c):          # W2-B: re-check in the same transaction
                raise LedgerError("ledger_audit_failed")
            return {"attempt_id": attempt_id, "from": st, "to": new_st, "delta_nano": new_ch - ch}
        return self.tx(f, timeout)

    def resume(self, *, actor, note, next_reserve_nano, ack_over_reservation=False, timeout=5.0) -> dict:
        """Deterministic re-check; resolves remaining stops only if every condition holds."""
        if not note or len(note.strip()) < 8:
            raise LedgerError("note_required")

        def f(c):
            reasons = []
            try:
                qc = c.execute("PRAGMA quick_check").fetchone()[0]
            except sqlite3.DatabaseError:
                qc = "error"
            if qc != "ok":
                reasons.append("quick_check_failed")
            probs = self._financial_problems(c)      # W2-B: same BEGIN IMMEDIATE transaction as the UPDATE
            if probs:
                reasons.append("audit_failed n=%d first=%s" % (len(probs), probs[0][:60]))
            n_hold = c.execute("SELECT COUNT(*) FROM attempts WHERE status IN (%s)" % ",".join("?" * len(HOLD_UNRESOLVED)),
                               HOLD_UNRESOLVED).fetchone()[0]
            if n_hold:
                reasons.append("unresolved_unknown_or_usage_invalid=%d" % n_hold)
            n_open = c.execute("SELECT COUNT(*) FROM attempts WHERE status IN ('RESERVED','SENDING')").fetchone()[0]
            if n_open:
                reasons.append("open_attempts=%d (run orphans first)" % n_open)
            kinds = self._open_stops(c)
            if "OVER_RESERVATION" in kinds and not ack_over_reservation:
                reasons.append("over_reservation_needs_explicit_ack")
            cap, exp = self._cap(c), self._exposure(c)
            if exp + int(next_reserve_nano) > cap:
                reasons.append("cannot_reserve_next exposure=%s next=%s cap=%s"
                               % (nano_to_usd(exp), nano_to_usd(next_reserve_nano), nano_to_usd(cap)))
            if reasons:
                return {"resumed": False, "reasons": reasons, "open_stops": kinds}
            n = c.execute("UPDATE stops SET resolved_ts=?, resolved_by=?, resolve_note=? WHERE resolved_ts IS NULL",
                          (now_iso(), actor, "resume: " + note)).rowcount
            # W2-D: an audited resume starts a new 4xx streak; the Retry-After pause is NOT cleared
            c.execute("UPDATE route_state SET consec_4xx=0, updated_ts=?", (now_iso(),))
            c.execute("INSERT INTO route_events(route, ts, kind, consec_after) SELECT route, ?, 'resume_reset', 0 FROM route_state",
                      (now_iso(),))
            return {"resumed": True, "stops_resolved": n, "exposure_usd": nano_to_usd(exp), "cap_usd": nano_to_usd(cap)}
        return self.tx(f, timeout)

    def migrate_v2_to_v3(self, *, actor, note, timeout=5.0) -> dict:
        """Explicit, authorised schema migration (adds route_state/route_events). Refuses unless the
        ledger is schema 2 and passes the financial audit first."""
        if not note or len(note.strip()) < 8:
            raise LedgerError("note_required")

        def f(c):
            meta = dict(c.execute("SELECT k, v FROM meta"))
            if meta.get("schema_version") != "2":
                raise LedgerError("migrate_needs_schema_2")
            if c.execute("PRAGMA quick_check").fetchone()[0] != "ok" or self._financial_problems(c):
                raise LedgerError("ledger_audit_failed")
            for stmt in [x for x in SCHEMA_V3_ADD.split(";") if x.strip()]:
                c.execute(stmt)
            c.execute("INSERT INTO route_state(route, consec_4xx, updated_ts) VALUES(?, 0, ?)", (meta.get("route"), now_iso()))
            c.execute("UPDATE meta SET v=? WHERE k='schema_version'", (SCHEMA_VERSION,))
            c.execute("INSERT INTO meta(k, v) VALUES(?, ?)", ("migrated_v3", "%s by %s: %s" % (now_iso(), actor, note)))
            return {"migrated": True, "schema_version": SCHEMA_VERSION}
        return self.tx(f, timeout)

    def list_attempts(self, *, status=None, run_id=None, limit=50, timeout=5.0) -> list:
        q, args = "SELECT * FROM attempts WHERE 1=1", []
        if status:
            q += " AND status=?"; args.append(status)
        if run_id:
            q += " AND run_id=?"; args.append(run_id)
        q += " ORDER BY ts_reserved DESC, slot LIMIT ?"; args.append(int(limit))

        def f(c):
            c.row_factory = sqlite3.Row
            return [dict(r) for r in c.execute(q, args)]
        return self.tx(f, timeout, write=False)

    def backup(self, out_path, timeout=10.0) -> dict:
        """sqlite online backup + integrity check of the copy + logical-dump sha256 equality."""
        out = Path(out_path)
        if out.exists():
            raise LedgerError("backup_target_exists")
        src = self._connect(timeout)
        try:
            dst = sqlite3.connect(str(out))
            try:
                src.backup(dst)
                ic = dst.execute("PRAGMA integrity_check").fetchone()[0]
                d_dst = hashlib.sha256("\n".join(dst.iterdump()).encode()).hexdigest()
            finally:
                dst.close()
            d_src = hashlib.sha256("\n".join(src.iterdump()).encode()).hexdigest()
        finally:
            src.close()
        fd = os.open(str(out), os.O_RDONLY)
        os.fsync(fd); os.close(fd)
        return {"backup": str(out), "integrity": ic, "dump_sha256_src": d_src, "dump_sha256_backup": d_dst,
                "verified": ic == "ok" and d_src == d_dst}
