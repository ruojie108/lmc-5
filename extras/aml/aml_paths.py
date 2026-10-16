"""Shared runtime paths; no credentials or mutable data belong in the checkout."""
import os
from pathlib import Path

STATE_DIR = Path(os.environ.get("AML_STATE_DIR", str(Path.home() / ".local/state/lmc5-aml"))).expanduser().resolve()
SECRETS_DIR = STATE_DIR / "secrets"
PAID_INSTANCE_LOCK = STATE_DIR / "run/rerank_paid.instance.lock"
