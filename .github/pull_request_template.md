## What and why

<!-- One or two sentences. Link the issue: Fixes #123 -->

## How I tested it

<!-- e.g. new unit test, `make demo`, a dry run against a real fleet (redacted output welcome) -->

## Checklist

- [ ] `make fmt lint test` passes
- [ ] Dry run still touches nothing; node operations check `job.Stopped()` (see CLAUDE.md invariants)
- [ ] No new dependencies in the core packages (backend-specific ones stay inside the backend)
- [ ] Changed a default (workers, fail-fast, dry-run)? Explained why above
- [ ] Docs updated (README / docs/extending.md / flag help) if behaviour changed
- [ ] Added myself to AUTHORS (first contribution)
