"""WO2 rerank budget ledger admin (revision 3). Metadata only; never reads or prints secrets.

Read-only:
  show      --ledger P                       summary: grants, cap, exposure, stops, by-status
  attempts  --ledger P [--status S] [--run R] [--limit N]    per-attempt ledger rows
  audit     --ledger P                       integrity_check + financial invariants (charge == SUM(adjustments) ...)
  route     --ledger P                       W2-D route health: consecutive 4xx counter, persisted Retry-After pause
Mutating (all refuse while a live rerank instance holds the instance lock; all need --actor):
  init      --ledger P --budget-id ID --route paid|test --grant-usd X --note T --i-authorize
  grant     --ledger P --usd X --note T --i-authorize
  import-history --ledger P --status HISTORY_SETTLED|HISTORY_UNKNOWN|EXTERNAL_ESTIMATE --usd X
                 [--tokens N] --model M --note T
  orphans   --ledger P                        RESERVED orphans -> released (provably unsent), SENDING -> UNKNOWN
  resolve   --ledger P --attempt ID --action keep|release|settle --evidence T [--tokens N --price-nano-per-token K]
  resume    --ledger P --note T [--next-reserve-usd X] [--ack-over-reservation] --i-authorize
  backup    --ledger P --out FILE                online backup + integrity + dump-hash verification
  migrate   --ledger P --note T --i-authorize    schema 2 -> 3 (adds route_state/route_events); refuses unless
                                                 the ledger passes the financial audit first (rev3)
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from aml_paths import STATE_DIR, SECRETS_DIR, PAID_INSTANCE_LOCK

sys.path.insert(0, str(Path(__file__).resolve().parent))   # J50: this directory
import rerank_ledger_jev as L  # noqa: E402  (J50 ruling F1-a: copy of shell/rerank_ledger.py + jev price)

CF_ACCOUNT = os.environ.get("AML_CF_ACCOUNT_ID", "")
ROUTES = {"paid": "cloudflare:" + CF_ACCOUNT + ":workers-ai", "test": "test:loopback", "jev": "typesafe:systemone"}


def _route_of(ledger: L.Ledger) -> str:
    return ledger.tx(lambda c: dict(c.execute("SELECT k, v FROM meta")).get("route"), 5.0, write=False)


def _hold_instance_lock(ledger_path: Path, route: str):
    """Mutations only when no live rerank instance uses this ledger's lock."""
    lp = PAID_INSTANCE_LOCK if route in (ROUTES["paid"], ROUTES["jev"]) else Path(str(ledger_path.resolve()) + ".instance.lock")
    lp.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lp), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise SystemExit("REFUSED: a live rerank instance holds %s; stop it first" % lp.name)
    return fd


def out(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=1, default=str))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd")
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--actor", default=os.environ.get("USER", "admin"))
    ap.add_argument("--budget-id"); ap.add_argument("--route", choices=list(ROUTES))
    ap.add_argument("--grant-usd"); ap.add_argument("--usd"); ap.add_argument("--note")
    ap.add_argument("--i-authorize", action="store_true")
    ap.add_argument("--status"); ap.add_argument("--run"); ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--tokens", type=int); ap.add_argument("--model")
    ap.add_argument("--attempt"); ap.add_argument("--action", choices=["keep", "release", "settle"])
    ap.add_argument("--evidence"); ap.add_argument("--price-nano-per-token", type=int)
    ap.add_argument("--next-reserve-usd", default="0.01"); ap.add_argument("--ack-over-reservation", action="store_true")
    ap.add_argument("--out")
    a = ap.parse_args(argv)
    lp = Path(a.ledger)
    try:
        if a.cmd == "init":
            if not (a.i_authorize and a.budget_id and a.route and a.grant_usd and a.note):
                raise SystemExit("init needs --budget-id --route --grant-usd --note --i-authorize")
            led = L.Ledger.init_new(lp, budget_id=a.budget_id, route=ROUTES[a.route],
                                    grant_nano=L.usd_to_nano(a.grant_usd), actor=a.actor, note=a.note)
            return out({"init": str(lp), **led.snapshot()})
        led = L.Ledger(lp)
        if a.cmd == "show":
            meta = led.tx(lambda c: dict(c.execute("SELECT k, v FROM meta")), 5.0, write=False)
            grants = led.tx(lambda c: [dict(zip(("id", "ts", "nano", "actor", "note"), r))
                                       for r in c.execute("SELECT id, ts, nano, actor, note FROM grants")], 5.0, write=False)
            stops = led.tx(lambda c: [dict(zip(("id", "ts", "kind", "attempt_id", "run_id", "note", "resolved_ts",
                                                "resolved_by", "resolve_note"), r))
                                      for r in c.execute("SELECT * FROM stops ORDER BY id")], 5.0, write=False)
            runs = led.tx(lambda c: [r[0] for r in c.execute("SELECT run_id FROM runs ORDER BY ts_start")], 5.0, write=False)
            for g in grants:
                g["usd"] = L.nano_to_usd(g["nano"])
            try:
                route_state = led.route_state()
            except L.LedgerError as e:
                route_state = {"error": e.code}
            return out({"meta": {k: v for k, v in meta.items() if k != "ledger_uuid"}, "grants": grants,
                        "summary": led.snapshot(), "stops": stops, "runs": runs, "route_state": route_state})
        if a.cmd == "attempts":
            rows = led.list_attempts(status=a.status, run_id=a.run, limit=a.limit)
            for r in rows:
                r["est_usd"] = L.nano_to_usd(r["est_nano"]); r["charge_usd"] = L.nano_to_usd(r["charge_nano"])
            return out(rows)
        if a.cmd == "audit":
            r = led.audit()
            out(r)
            return 0 if r["ok"] else 5
        if a.cmd == "route":
            return out(led.route_state())
        # ---- mutating ----
        route = _route_of(led)
        _fd = _hold_instance_lock(lp, route)
        if a.cmd == "grant":
            if not (a.i_authorize and a.usd and a.note):
                raise SystemExit("grant needs --usd --note --i-authorize")
            led.add_grant(L.usd_to_nano(a.usd), a.actor, a.note)
            return out(led.snapshot())
        if a.cmd == "import-history":
            if not (a.status and a.usd and a.model and a.note):
                raise SystemExit("import-history needs --status --usd --model --note")
            aid = led.import_history(status=a.status, nano=L.usd_to_nano(a.usd, ceil=True), tokens=a.tokens,
                                     model=a.model, actor=a.actor, note=a.note)
            return out({"attempt_id": aid, **led.snapshot()})
        if a.cmd == "orphans":
            return out(led.recover_orphans("admin-orphan-sweep", actor=a.actor))
        if a.cmd == "resolve":
            if not (a.attempt and a.action and a.evidence):
                raise SystemExit("resolve needs --attempt --action --evidence")
            return out(led.resolve_attempt(a.attempt, action=a.action, actor=a.actor, evidence=a.evidence,
                                           tokens=a.tokens, price_nano_per_token=a.price_nano_per_token))
        if a.cmd == "resume":
            if not (a.i_authorize and a.note):
                raise SystemExit("resume needs --note --i-authorize")
            r = led.resume(actor=a.actor, note=a.note, next_reserve_nano=L.usd_to_nano(a.next_reserve_usd, ceil=True),
                           ack_over_reservation=a.ack_over_reservation)
            out(r)
            return 0 if r["resumed"] else 3
        if a.cmd == "migrate":
            if not (a.i_authorize and a.note):
                raise SystemExit("migrate needs --note --i-authorize")
            return out(led.migrate_v2_to_v3(actor=a.actor, note=a.note))
        if a.cmd == "backup":
            if not a.out:
                raise SystemExit("backup needs --out")
            r = led.backup(a.out)
            out(r)
            return 0 if r["verified"] else 4
        raise SystemExit("unknown command " + a.cmd)
    except L.LedgerError as e:
        out({"error": e.code})
        return 2


if __name__ == "__main__":
    rc = main()
    sys.exit(rc if isinstance(rc, int) else 0)
