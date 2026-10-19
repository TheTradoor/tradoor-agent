"""Phase 3 — Execution via PumpPortal local-tx API: build -> simulate -> sign locally -> send -> confirm -> reconcile.
Idempotency key per decision so a retry can never double-buy. confirm mode asks owner in Telegram first."""
