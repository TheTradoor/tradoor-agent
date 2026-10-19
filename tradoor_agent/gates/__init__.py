"""Phase 1 — Hard gates: cheap deterministic checks on each token
(bundle, dev/top-10 holdings, mint/freeze authority, LP, sellability sim).
Input: token snapshot. Output: list[GateResult]. Any fail -> drop, never scored."""
