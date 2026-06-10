# CLAUDE.md

Agent instructions live in **`AGENTS.md`** (and `.github/copilot-instructions.md`). Read those.

## ⚠️ DO NOT REGRESS

Before editing **device-identity, retry/reliability, licensed-mode, or USB-CDC** code,
read **[`docs/RELIABILITY_INVARIANTS.md`](docs/RELIABILITY_INVARIANTS.md)**. It lists
hard-won fixes (node-number anti-churn, licensed-mode-off, 24 h DM retry, 24 h per-member
group retry, ~1 h media retry, non-blocking USB-CDC, and test-harness rules) with the
exact files and the real bug each one prevents. A "cleanup" that reverts any of them
will reintroduce an observed failure.
