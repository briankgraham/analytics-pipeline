## What and why

## Checklist (docs/design.md)
- [ ] Failure modes considered: crash, duplicate delivery, late data, overload, bad input
- [ ] Idempotent (replay and retries are safe)
- [ ] Backpressure, timeouts, and retries are bounded
- [ ] New config validated at boot and documented
- [ ] Tests cover the failure path, not only the happy path
- [ ] Observability added (structured logs, metrics)
- [ ] Docs, ADR, `CLAUDE.md` decision log, and `README.md` updated
- [ ] Every commit has a `Review:` line from a fresh-context review against `CODE_REVIEW.md`
