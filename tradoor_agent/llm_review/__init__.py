"""Phase 2 — LLM grey-zone review. Structured input, strict JSON verdict, timeout -> NEUTRAL,
confidence < min -> NEUTRAL, boost capped by LLM_MAX_BOOST, verdict cached per mint.
Token-supplied text is passed as untrusted data. Default is always: do not buy."""
